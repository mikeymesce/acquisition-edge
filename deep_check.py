#!/usr/bin/env python3
"""
Deep Check — Google search each contractor for distress signals.

Searches Google for each Master Plumber and Electrical Contractor,
extracts ratings, review counts, website presence, and distress keywords
from search result snippets. Then re-scores with the two-score model.

Usage:
    python3 deep_check.py
"""

import os
import re
import sys
import json
import time
import random
import sqlite3
from datetime import datetime
from playwright.sync_api import sync_playwright

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "contractors.db")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

DISTRESS_KEYWORDS = [
    "closed", "out of business", "retired", "for sale",
    "obituary", "passed away", "divorce", "lawsuit",
    "violation", "permanently closed", "shutting down",
]

TARGET_LICENSE_TYPES = ["Master Plumber", "Electrical Contractor"]


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def add_columns_if_missing(conn):
    """Add google_rating, review_count, has_website, score columns if they don't exist."""
    new_cols = [
        ("google_rating", "REAL"),
        ("review_count", "INTEGER"),
        ("has_website", "INTEGER DEFAULT 0"),
        ("distress_signals", "TEXT"),
        ("quality_score", "INTEGER DEFAULT 0"),
        ("motivation_score", "INTEGER DEFAULT 0"),
        ("combined_score", "REAL DEFAULT 0"),
    ]
    for col_name, col_type in new_cols:
        try:
            conn.execute(f"ALTER TABLE contractors ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name}")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()


def get_contractors(conn):
    """Load Master Plumber and Electrical Contractor records."""
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in TARGET_LICENSE_TYPES)
    rows = conn.execute(
        f"SELECT * FROM contractors WHERE license_type IN ({placeholders})",
        TARGET_LICENSE_TYPES,
    ).fetchall()
    return [dict(r) for r in rows]


def launch_browser(playwright):
    """Launch Chromium with anti-detection settings (same as NJ scraper)."""
    browser = playwright.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )
    page = context.new_page()
    page.add_init_script(
        'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
    )
    return browser, context, page


def search_google(page, query):
    """Run a Google search and return the page HTML."""
    encoded = query.replace(" ", "+")
    url = f"https://www.google.com/search?q={encoded}"
    try:
        page.goto(url, timeout=15000, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        return page.content()
    except Exception as e:
        print(f"    [ERROR] Google search failed: {e}")
        return ""


def parse_google_results(html):
    """Extract rating, review count, website presence, and distress signals from Google HTML."""
    result = {
        "google_rating": None,
        "review_count": None,
        "has_website": 0,
        "distress_signals": [],
    }

    if not html:
        return result

    text_lower = html.lower()

    # --- Google Maps rating ---
    rating_patterns = [
        r'(\d\.\d)\s*\((\d[\d,]*)\)',
        r'[Rr]ated\s+(\d\.\d)\s+out\s+of\s+5',
        r'(\d\.\d)\s*stars?',
    ]

    for pattern in rating_patterns:
        match = re.search(pattern, html)
        if match:
            try:
                result["google_rating"] = float(match.group(1))
                if match.lastindex >= 2:
                    count_str = match.group(2).replace(",", "")
                    result["review_count"] = int(count_str)
            except (ValueError, IndexError):
                pass
            break

    if result["google_rating"] and not result["review_count"]:
        review_match = re.search(r'\((\d[\d,]*)\s*(?:reviews?|Google reviews?)\)', html, re.IGNORECASE)
        if review_match:
            try:
                result["review_count"] = int(review_match.group(1).replace(",", ""))
            except ValueError:
                pass

    # --- Website presence ---
    if re.search(r'class="[^"]*"[^>]*>Website<', html, re.IGNORECASE):
        result["has_website"] = 1

    directory_domains = [
        "yelp.com", "yellowpages.com", "bbb.org", "angi.com",
        "homeadvisor.com", "facebook.com", "linkedin.com", "mapquest.com",
        "manta.com", "google.com", "nextdoor.com", "thumbtack.com",
    ]
    url_matches = re.findall(r'href="(https?://[^"]+)"', html)
    for u in url_matches:
        u_lower = u.lower()
        if not any(d in u_lower for d in directory_domains):
            if ".com" in u_lower or ".net" in u_lower or ".org" in u_lower:
                result["has_website"] = 1
                break

    # --- Distress signals from snippets ---
    for keyword in DISTRESS_KEYWORDS:
        if keyword in text_lower:
            idx = text_lower.find(keyword)
            start = max(0, idx - 100)
            end = min(len(text_lower), idx + len(keyword) + 100)
            context = text_lower[start:end]
            if "did you mean" not in context and "related searches" not in context:
                result["distress_signals"].append(keyword)

    return result


def rescore_contractor(row, google_data):
    """Re-score a contractor using the two-score model after Google enrichment."""

    status = (row.get("status") or "").lower()
    signals = google_data.get("distress_signals", [])
    signal_str = ",".join(signals)
    has_website = google_data.get("has_website", 0)
    rating = google_data.get("google_rating")
    reviews = google_data.get("review_count")

    # --- Business Quality Score (0-100) ---
    quality = 0
    q_notes = []

    if "active" in status:
        quality += 15
        q_notes.append("Active license")

    if has_website:
        quality += 15
        q_notes.append("Has website")

    if rating is not None:
        if rating >= 4.0:
            quality += 20
            q_notes.append(f"Rating {rating}")
        elif rating >= 3.5:
            quality += 10
            q_notes.append(f"Rating {rating}")

    if reviews is not None:
        if reviews >= 100:
            quality += 20
            q_notes.append(f"{reviews} reviews")
        elif reviews >= 50:
            quality += 15
            q_notes.append(f"{reviews} reviews")
        elif reviews >= 10:
            quality += 10
            q_notes.append(f"{reviews} reviews")
        else:
            quality += 5
            q_notes.append(f"{reviews} reviews")

    quality = min(quality, 100)

    # --- Seller Motivation Score (0-100) ---
    motivation = 0
    m_notes = []

    all_text = f"{signal_str} {(row.get('notes') or '')}".lower()

    death_keywords = ["obituary", "passed away", "died", "estate", "probate"]
    if any(kw in all_text for kw in death_keywords):
        motivation += 30
        m_notes.append("Death/estate signal")

    if "divorce" in all_text:
        motivation += 25
        m_notes.append("Divorce signal")

    if "expired" in status or "lapsed" in status:
        motivation += 20
        m_notes.append("License expired")

    if "retired" in all_text or "retiring" in all_text:
        motivation += 20
        m_notes.append("Retiring")

    closed_keywords = ["out of business", "permanently closed", "for sale", "shutting down", "closed"]
    if any(kw in all_text for kw in closed_keywords):
        motivation += 20
        m_notes.append("Closed/for sale")

    if "revoked" in status or "suspended" in status:
        motivation += 15
        m_notes.append(f"License {status}")

    if not has_website and "active" in status:
        motivation += 5
        m_notes.append("No website but active")

    motivation = min(motivation, 100)

    # Combined
    combined = (quality * 0.6) + (motivation * 0.4)

    all_notes = q_notes + m_notes
    notes_str = "; ".join(all_notes) if all_notes else ""

    return quality, motivation, round(combined, 1), notes_str, signal_str


def _save_results(results):
    """Save top results to a text file."""
    os.makedirs("results", exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    filepath = f"results/deep_check_{today}.txt"

    results.sort(key=lambda x: x["combined"], reverse=True)

    with open(filepath, "w") as f:
        f.write(f"Deep Check Results — {today}\n")
        f.write("=" * 60 + "\n\n")

        for i, r in enumerate(results[:50], 1):
            f.write(f"{i}. {r['name']} — {r['city']}, NJ\n")
            f.write(f"   {r['license_type']}\n")
            f.write(f"   Quality: {r['quality']} | Motivation: {r['motivation']} | Combined: {r['combined']}\n")
            f.write(f"   Rating: {r['rating'] or 'N/A'} | Reviews: {r['reviews'] or 'N/A'} | Website: {'Yes' if r['has_website'] else 'No'}\n")
            if r["notes"]:
                f.write(f"   Signals: {r['notes']}\n")
            f.write("\n")

    print(f"\nResults saved to {filepath}")


def main():
    print("=" * 55)
    print("Deep Check — Google Distress Signal Scanner")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    add_columns_if_missing(conn)

    contractors = get_contractors(conn)
    print(f"\nLoaded {len(contractors)} contractors (Master Plumber + Electrical Contractor)")

    results = []

    with sync_playwright() as p:
        browser, context, page = launch_browser(p)

        for i, c in enumerate(contractors, 1):
            name = c["name"] or "Unknown"
            city = c["city"] or ""
            license_type = c["license_type"] or ""
            industry_kw = "plumbing" if "plumb" in license_type.lower() else "electrical"

            query = f"{name} {city} NJ {industry_kw}"
            print(f"\n[{i}/{len(contractors)}] Searching: {query}")

            html = search_google(page, query)

            # Check for CAPTCHA
            if "captcha" in html.lower() or "unusual traffic" in html.lower():
                print("  [WARNING] CAPTCHA detected! Pausing 30 seconds...")
                time.sleep(30)
                html = search_google(page, query)
                if "captcha" in html.lower() or "unusual traffic" in html.lower():
                    print("  [ERROR] Still blocked. Skipping remaining searches.")
                    break

            google_data = parse_google_results(html)

            # Print what we found
            r = google_data["google_rating"]
            rc = google_data["review_count"]
            hw = google_data["has_website"]
            sigs = google_data["distress_signals"]
            print(f"  Rating: {r or 'N/A'} | Reviews: {rc or 'N/A'} | Website: {'Yes' if hw else 'No'}")
            if sigs:
                print(f"  DISTRESS SIGNALS: {', '.join(sigs)}")

            # Two-score model
            quality, motivation, combined, notes, signal_str = rescore_contractor(c, google_data)
            print(f"  Quality: {quality} | Motivation: {motivation} | Combined: {combined}")

            # Update database with all new columns
            conn.execute("""
                UPDATE contractors SET
                    google_rating = ?,
                    review_count = ?,
                    has_website = ?,
                    distress_signals = ?,
                    quality_score = ?,
                    motivation_score = ?,
                    combined_score = ?,
                    distress_score = ?,
                    notes = ?,
                    last_updated = ?
                WHERE id = ?
            """, (
                google_data["google_rating"],
                google_data["review_count"],
                google_data["has_website"],
                signal_str or None,
                quality,
                motivation,
                combined,
                motivation,  # backwards compat
                notes or None,
                datetime.now().strftime("%Y-%m-%d"),
                c["id"],
            ))
            conn.commit()

            results.append({
                "name": name,
                "city": city,
                "license_type": license_type,
                "quality": quality,
                "motivation": motivation,
                "combined": combined,
                "notes": notes,
                "rating": google_data["google_rating"],
                "reviews": google_data["review_count"],
                "has_website": google_data["has_website"],
                "signals": sigs,
            })

            # Polite delay: 3-5 seconds between searches
            delay = random.uniform(3, 5)
            print(f"  Waiting {delay:.1f}s...")
            time.sleep(delay)

        browser.close()

    # --- Summary ---
    print(f"\n{'=' * 55}")
    print("DEEP CHECK COMPLETE")
    print(f"{'=' * 55}")
    print(f"Checked: {len(results)} contractors")

    # Sort by combined score descending
    results.sort(key=lambda x: x["combined"], reverse=True)

    # Top 10
    top = results[:10]
    if top:
        print(f"\nTop {len(top)} Leads (by Combined Score):")
        print("-" * 55)
        for i, r in enumerate(top, 1):
            print(f"\n  {i}. {r['name']} — {r['city']}, NJ")
            print(f"     {r['license_type']} | Combined: {r['combined']}")
            print(f"     Quality: {r['quality']} | Motivation: {r['motivation']}")
            print(f"     Rating: {r['rating'] or 'N/A'} | Reviews: {r['reviews'] or 'N/A'} | Website: {'Yes' if r['has_website'] else 'No'}")
            if r["notes"]:
                print(f"     Signals: {r['notes']}")
    else:
        print("\nNo results to show.")

    # Quick stats
    with_motivation = sum(1 for r in results if r["motivation"] > 0)
    with_rating = sum(1 for r in results if r["rating"] is not None)
    no_website = sum(1 for r in results if not r["has_website"])
    print(f"\nStats:")
    print(f"  Contractors with motivation signals: {with_motivation}")
    print(f"  Contractors with Google rating:      {with_rating}")
    print(f"  Contractors with no website:         {no_website}")
    print(f"{'=' * 55}")

    # Save results file
    if results:
        _save_results(results)

    conn.close()


if __name__ == "__main__":
    main()
