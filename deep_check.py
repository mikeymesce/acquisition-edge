#!/usr/bin/env python3
"""
Deep Check — Google search each contractor for distress signals.

Searches Google for each Master Plumber and Electrical Contractor,
extracts ratings, review counts, website presence, and distress keywords
from search result snippets.

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
    """Add google_rating, review_count, has_website columns if they don't exist."""
    new_cols = [
        ("google_rating", "REAL"),
        ("review_count", "INTEGER"),
        ("has_website", "INTEGER DEFAULT 0"),
        ("distress_signals", "TEXT"),
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
    # Google local pack shows ratings like "4.5" near star icons, often in spans
    # Pattern: "X.X" followed by stars or "(NNN)" review count
    # Look for the rating pattern in the local pack area
    rating_patterns = [
        # "4.5(123)" or "4.5 (123)"
        r'(\d\.\d)\s*\((\d[\d,]*)\)',
        # aria-label="Rated 4.5 out of 5" or "4.5 out of 5 stars"
        r'[Rr]ated\s+(\d\.\d)\s+out\s+of\s+5',
        # "4.5 stars" nearby "(123 reviews)"
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

    # If we got a rating but no review count, try to find review count separately
    if result["google_rating"] and not result["review_count"]:
        review_match = re.search(r'\((\d[\d,]*)\s*(?:reviews?|Google reviews?)\)', html, re.IGNORECASE)
        if review_match:
            try:
                result["review_count"] = int(review_match.group(1).replace(",", ""))
            except ValueError:
                pass

    # --- Website presence ---
    # If there's a non-Google, non-Yelp, non-directory link in top results, they probably have a site
    # Simple heuristic: look for "Website" link in local pack or a homepage-looking result
    if re.search(r'class="[^"]*"[^>]*>Website<', html, re.IGNORECASE):
        result["has_website"] = 1

    # Also check if any organic result looks like their own domain (not directories)
    directory_domains = [
        "yelp.com", "yellowpages.com", "bbb.org", "angi.com",
        "homeadvisor.com", "facebook.com", "linkedin.com", "mapquest.com",
        "manta.com", "google.com", "nextdoor.com", "thumbtack.com",
    ]
    # Find URLs in search results
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
            # Verify it's in actual content, not just in Google UI boilerplate
            # Find surrounding context
            idx = text_lower.find(keyword)
            # Grab a window around the keyword
            start = max(0, idx - 100)
            end = min(len(text_lower), idx + len(keyword) + 100)
            context = text_lower[start:end]
            # Skip if it's clearly in a Google UI element (search suggestions, etc.)
            if "did you mean" not in context and "related searches" not in context:
                result["distress_signals"].append(keyword)

    return result


def rescore_contractor(row, google_data, weights):
    """Re-score a contractor using existing license signals + new Google signals."""
    score = 0
    notes = []

    # --- Existing license-based scoring (from scoring.py logic) ---
    status = (row.get("status") or "").lower()
    if "expired" in status or "lapsed" in status:
        score += weights.get("license_expired", 25)
        notes.append("License expired")
    if "revoked" in status or "suspended" in status:
        score += weights.get("disciplinary_action", 20)
        notes.append(f"License {status}")

    exp_date_str = row.get("expiration_date") or ""
    if exp_date_str:
        try:
            today = datetime.now()
            for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]:
                try:
                    exp_date = datetime.strptime(exp_date_str.strip(), fmt)
                    break
                except ValueError:
                    continue
            else:
                exp_date = None
            if exp_date:
                days_until = (exp_date - today).days
                if 0 < days_until <= 90:
                    score += weights.get("license_expiring_90_days", 15)
                    notes.append(f"Expires in {days_until}d")
                elif days_until < 0 and "expired" not in status:
                    score += weights.get("license_expired", 25)
                    notes.append(f"Expired {abs(days_until)}d ago")
        except Exception:
            pass

    # --- Google-based signals ---
    rating = google_data.get("google_rating")
    reviews = google_data.get("review_count")
    has_website = google_data.get("has_website", 0)
    signals = google_data.get("distress_signals", [])

    # No website = small signal
    if not has_website:
        score += weights.get("no_website", 5)
        notes.append("No website found")

    # Low rating
    if rating is not None and rating < 3.0:
        score += 10
        notes.append(f"Low rating: {rating}")

    # Very few or no reviews (might mean tiny/fading business)
    if reviews is not None and reviews <= 3:
        score += 5
        notes.append(f"Only {reviews} reviews")

    # Distress keywords
    for signal in signals:
        if signal in ("obituary", "passed away"):
            score += weights.get("probate_filing", 25)
            notes.append(f"Google: {signal}")
        elif signal == "divorce":
            score += weights.get("divorce_filing", 20)
            notes.append(f"Google: {signal}")
        elif signal in ("closed", "out of business", "permanently closed"):
            score += 20
            notes.append(f"Google: {signal}")
        elif signal in ("for sale", "retired", "shutting down"):
            score += 15
            notes.append(f"Google: {signal}")
        elif signal in ("lawsuit", "violation"):
            score += 10
            notes.append(f"Google: {signal}")

    return score, "; ".join(notes), ",".join(signals)


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

    config = load_config()
    weights = config.get("distress_weights", {})

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

            # Rescore
            score, notes, signal_str = rescore_contractor(c, google_data, weights)

            # Update database
            conn.execute("""
                UPDATE contractors SET
                    google_rating = ?,
                    review_count = ?,
                    has_website = ?,
                    distress_signals = ?,
                    distress_score = ?,
                    notes = ?,
                    last_updated = ?
                WHERE id = ?
            """, (
                google_data["google_rating"],
                google_data["review_count"],
                google_data["has_website"],
                signal_str or None,
                score,
                notes or None,
                datetime.now().strftime("%Y-%m-%d"),
                c["id"],
            ))
            conn.commit()

            results.append({
                "name": name,
                "city": city,
                "license_type": license_type,
                "score": score,
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

    # Sort by score descending
    results.sort(key=lambda x: x["score"], reverse=True)

    # Top 10 most distressed
    top = results[:10]
    if top:
        print(f"\nTop {len(top)} Most Distressed:")
        print("-" * 55)
        for i, r in enumerate(top, 1):
            print(f"\n  {i}. {r['name']} — {r['city']}, NJ")
            print(f"     {r['license_type']} | Score: {r['score']}")
            print(f"     Rating: {r['rating'] or 'N/A'} | Reviews: {r['reviews'] or 'N/A'} | Website: {'Yes' if r['has_website'] else 'No'}")
            if r["notes"]:
                print(f"     Signals: {r['notes']}")
    else:
        print("\nNo results to show.")

    # Quick stats
    with_signals = sum(1 for r in results if r["score"] > 0)
    with_rating = sum(1 for r in results if r["rating"] is not None)
    no_website = sum(1 for r in results if not r["has_website"])
    print(f"\nStats:")
    print(f"  Contractors with distress signals: {with_signals}")
    print(f"  Contractors with Google rating:    {with_rating}")
    print(f"  Contractors with no website:       {no_website}")
    print(f"{'=' * 55}")

    conn.close()


if __name__ == "__main__":
    main()
