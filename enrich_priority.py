#!/usr/bin/env python3
"""
Enrich Priority Leads — Deceased & Retired NJ Contractors

Two-pass enrichment:
  Pass 1: Scrape NJ license detail pages for issue date, business name, expiration
  Pass 2: 3 Google searches per person (obituary, business info, contact/LinkedIn)

Cross-checks sources and flags actionable leads.

Usage:
    python3 enrich_priority.py
"""

import os
import re
import sys
import time
import random
import sqlite3
import functools
from datetime import datetime
from urllib.parse import quote_plus
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Force unbuffered output so we can see progress in real time
print = functools.partial(print, flush=True)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "contractors.db")
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# ── Column definitions to add ───────────────────────────────────────────────
NEW_COLUMNS = [
    ("issue_date", "TEXT"),
    ("business_phone", "TEXT"),
    ("business_address", "TEXT"),
    ("business_website", "TEXT"),
    ("business_type", "TEXT"),
    ("employee_count", "INTEGER"),
    ("death_year", "INTEGER"),
    ("obituary_found", "INTEGER DEFAULT 0"),
    ("family_contacts", "TEXT"),
    ("linkedin_url", "TEXT"),
    ("personal_phone", "TEXT"),
    ("business_still_active", "INTEGER DEFAULT 0"),
    ("verified", "INTEGER DEFAULT 0"),
    ("needs_review", "INTEGER DEFAULT 0"),
    ("enrichment_notes", "TEXT"),
    # Also add deep_check columns if missing
    ("google_rating", "REAL"),
    ("review_count", "INTEGER"),
    ("has_website", "INTEGER DEFAULT 0"),
    ("distress_signals", "TEXT"),
]


def add_columns(conn):
    """Add enrichment columns to the contractors table."""
    for col_name, col_type in NEW_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE contractors ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name}")
        except sqlite3.OperationalError:
            pass  # already exists
    conn.commit()


def get_priority_contractors(conn):
    """Get deceased first, then retired — ordered for processing."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM contractors
        WHERE status IN ('Deceased', 'Retired')
        ORDER BY
            CASE status WHEN 'Deceased' THEN 1 WHEN 'Retired' THEN 2 END,
            name
    """).fetchall()
    return [dict(r) for r in rows]


def launch_browser(playwright):
    """Launch Chromium with anti-detection (matches nj_licenses.py)."""
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


# ═══════════════════════════════════════════════════════════════════════════
# PASS 1: Scrape NJ license detail pages
# ═══════════════════════════════════════════════════════════════════════════

def scrape_detail_page(page, url):
    """Visit a NJ MyLicense detail page and extract all available fields."""
    result = {
        "issue_date": None,
        "expiration_date": None,
        "business_name": None,
        "detail_fields": {},
    }
    if not url:
        return result

    try:
        page.goto(url, timeout=20000, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        html = page.content()
    except Exception as e:
        print(f"    [ERROR] Detail page failed: {e}")
        return result

    soup = BeautifulSoup(html, "html.parser")

    # The detail page has label/value pairs — extract them all
    # Look for table cells or div pairs with labels
    all_text = soup.get_text(" ", strip=True)

    # Try to find structured label:value pairs
    # NJ MyLicense uses spans with IDs like _ctl##_lbl* and _ctl##_value*
    labels = soup.find_all("span", id=re.compile(r"lbl", re.IGNORECASE))
    for label_span in labels:
        label_text = label_span.get_text(strip=True).rstrip(":")
        # The value is usually in the next sibling or a nearby span
        parent_td = label_span.find_parent("td")
        if parent_td:
            next_td = parent_td.find_next_sibling("td")
            if next_td:
                value = next_td.get_text(strip=True)
                if value:
                    result["detail_fields"][label_text] = value

    # Also try generic table row parsing
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) == 2:
            label = cells[0].get_text(strip=True).rstrip(":")
            value = cells[1].get_text(strip=True)
            if label and value and label not in result["detail_fields"]:
                result["detail_fields"][label] = value

    # Extract specific fields from whatever we found
    fields = result["detail_fields"]
    for key, val in fields.items():
        key_lower = key.lower()
        if "issue" in key_lower and "date" in key_lower:
            result["issue_date"] = val
        elif "expir" in key_lower and "date" in key_lower:
            result["expiration_date"] = val
        elif "business" in key_lower and "name" in key_lower:
            result["business_name"] = val
        elif "firm" in key_lower or "company" in key_lower or "employer" in key_lower:
            if not result["business_name"]:
                result["business_name"] = val

    # Fallback: search raw text for dates near keywords
    if not result["issue_date"]:
        m = re.search(r'[Ii]ssue\s*[Dd]ate[:\s]*(\d{1,2}/\d{1,2}/\d{4})', all_text)
        if m:
            result["issue_date"] = m.group(1)

    if not result["expiration_date"]:
        m = re.search(r'[Ee]xpir\w*\s*[Dd]ate[:\s]*(\d{1,2}/\d{1,2}/\d{4})', all_text)
        if m:
            result["expiration_date"] = m.group(1)

    return result


def run_pass1(page, contractors, conn):
    """Pass 1: Visit each contractor's detail page."""
    print("\n" + "=" * 60)
    print("PASS 1: Scraping NJ License Detail Pages")
    print("=" * 60)

    consecutive_errors = 0
    extracted_count = 0

    for i, c in enumerate(contractors, 1):
        name = c["name"] or "Unknown"
        city = c["city"] or ""
        url = c.get("detail_url") or ""

        print(f"[{i}/{len(contractors)}] {name} — {city}, NJ")

        if not url:
            print("    No detail URL, skipping")
            continue

        detail = scrape_detail_page(page, url)

        # Check if we got anything useful
        has_data = bool(detail["issue_date"] or detail["expiration_date"]
                       or detail["business_name"] or detail["detail_fields"])

        if not has_data:
            consecutive_errors += 1
            if consecutive_errors >= 5:
                print(f"\n    *** {consecutive_errors} consecutive pages with no data ***")
                print(f"    NJ MyLicense detail pages appear to be down/blocked.")
                print(f"    Skipping rest of Pass 1 — moving to Google searches.")
                break
            continue
        else:
            consecutive_errors = 0
            extracted_count += 1

        # Update database
        updates = {}
        if detail["issue_date"]:
            updates["issue_date"] = detail["issue_date"]
            print(f"    Issue date: {detail['issue_date']}")
        if detail["expiration_date"] and not c.get("expiration_date"):
            updates["expiration_date"] = detail["expiration_date"]
            print(f"    Expiration: {detail['expiration_date']}")
        if detail["business_name"] and not c.get("business_name"):
            updates["business_name"] = detail["business_name"]
            print(f"    Business: {detail['business_name']}")

        if detail["detail_fields"]:
            extras = "; ".join(f"{k}: {v}" for k, v in detail["detail_fields"].items()
                              if k.lower() not in ("", "name", "license number"))
            if extras:
                updates["enrichment_notes"] = extras[:500]

        if updates:
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            values = list(updates.values()) + [c["id"]]
            conn.execute(f"UPDATE contractors SET {set_clause} WHERE id = ?", values)
            conn.commit()

        # Also update the in-memory dict so Pass 2 has latest data
        for k, v in updates.items():
            c[k] = v

        # Polite delay
        time.sleep(random.uniform(1, 2))

    print(f"\nPass 1 complete. Extracted data from {extracted_count} pages.")


# ═══════════════════════════════════════════════════════════════════════════
# PASS 2: Google searches (3 per person)
# ═══════════════════════════════════════════════════════════════════════════

def google_search(page, query):
    """Run a Google search and return (html, was_captcha)."""
    encoded = quote_plus(query)
    url = f"https://www.google.com/search?q={encoded}&num=10"
    try:
        page.goto(url, timeout=15000, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        html = page.content()
    except Exception as e:
        print(f"    [ERROR] Google search failed: {e}")
        return "", False

    lower = html.lower()
    if "captcha" in lower or "unusual traffic" in lower or "recaptcha" in lower:
        return html, True

    return html, False


def extract_phone_numbers(text):
    """Extract US phone numbers from text."""
    patterns = [
        r'\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}',
    ]
    phones = []
    for p in patterns:
        for m in re.finditer(p, text):
            num = re.sub(r'[^\d]', '', m.group())
            if len(num) == 10 and num[0] not in ('0', '1'):
                phones.append(m.group().strip())
    return list(set(phones))


def extract_year(text, name_parts):
    """Try to extract a death year from obituary text."""
    # Look for patterns like "died in 2023", "passed away 2024", "(1945-2023)"
    current_year = datetime.now().year

    # Birth-death range pattern
    m = re.search(r'\b(19\d{2}|20[0-2]\d)\s*[-–—]\s*(20[0-2]\d)\b', text)
    if m:
        year = int(m.group(2))
        if 2000 <= year <= current_year:
            return year

    # "died" / "passed away" followed by year or date
    for keyword in ["died", "passed away", "passed on", "passing"]:
        idx = text.lower().find(keyword)
        if idx >= 0:
            snippet = text[idx:idx+100]
            m = re.search(r'\b(20[0-2]\d)\b', snippet)
            if m:
                year = int(m.group(1))
                if 2000 <= year <= current_year:
                    return year

    # Any year near the person's name
    for part in name_parts:
        if len(part) < 3:
            continue
        idx = text.lower().find(part.lower())
        if idx >= 0:
            snippet = text[max(0, idx-50):idx+200]
            m = re.search(r'\b(20[12]\d)\b', snippet)
            if m:
                year = int(m.group(1))
                if 2000 <= year <= current_year:
                    return year

    return None


def extract_family_names(text, contractor_name):
    """Extract potential family member names from obituary snippets."""
    # Look for patterns like "survived by wife Jane", "son John Smith"
    family = []
    name_parts = contractor_name.lower().split()

    family_keywords = [
        r'(?:wife|husband|spouse|partner)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
        r'(?:son|daughter|brother|sister)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
        r'(?:survived by|leaves behind|mourned by)\s+.*?([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    ]

    for pattern in family_keywords:
        for m in re.finditer(pattern, text):
            name = m.group(1).strip()
            if name.lower() not in name_parts and len(name) > 3:
                family.append(name)

    return list(set(family))[:5]  # Cap at 5


def parse_obituary_search(html, name, city):
    """Parse Google results for obituary search."""
    result = {
        "death_year": None,
        "obituary_found": 0,
        "family_contacts": [],
        "obit_snippet": "",
    }
    if not html:
        return result

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    name_parts = name.split()

    # Check for obituary indicators
    obit_keywords = ["obituary", "obit", "passed away", "died", "memorial", "funeral",
                     "survived by", "in loving memory", "rest in peace"]
    text_lower = text.lower()

    for kw in obit_keywords:
        if kw in text_lower:
            result["obituary_found"] = 1
            break

    # Extract death year
    if result["obituary_found"]:
        result["death_year"] = extract_year(text, name_parts)
        result["family_contacts"] = extract_family_names(text, name)

        # Get a snippet
        for kw in ["obituary", "passed away", "died"]:
            idx = text_lower.find(kw)
            if idx >= 0:
                start = max(0, idx - 50)
                end = min(len(text), idx + 200)
                result["obit_snippet"] = text[start:end].strip()
                break

    return result


def parse_business_search(html, name, city):
    """Parse Google results for business info."""
    result = {
        "business_phone": None,
        "business_address": None,
        "google_rating": None,
        "review_count": None,
        "employee_count": None,
        "business_still_active": 0,
        "business_website": None,
        "business_type": None,
        "has_website": 0,
    }
    if not html:
        return result

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text_lower = text.lower()

    # Phone numbers
    phones = extract_phone_numbers(text)
    if phones:
        result["business_phone"] = phones[0]

    # Address — look for NJ addresses
    addr_pattern = r'\d+\s+[A-Za-z\s]+(?:St|Ave|Blvd|Rd|Dr|Ln|Way|Ct|Pl|Ter|Pkwy|Hwy)[.,]?\s+[A-Za-z\s]+,?\s*NJ\s*\d{5}'
    addr_match = re.search(addr_pattern, text)
    if addr_match:
        result["business_address"] = addr_match.group().strip()

    # Google rating
    rating_patterns = [
        r'(\d\.\d)\s*\((\d[\d,]*)\)',
        r'(\d\.\d)\s*stars?\s*[·•]\s*(\d[\d,]*)\s*reviews?',
        r'[Rr]ated\s+(\d\.\d)',
    ]
    for pattern in rating_patterns:
        m = re.search(pattern, text)
        if m:
            try:
                result["google_rating"] = float(m.group(1))
                if m.lastindex >= 2:
                    result["review_count"] = int(m.group(2).replace(",", ""))
            except (ValueError, IndexError):
                pass
            break

    # Employee count
    emp_patterns = [
        r'(\d+)\s*(?:[-–]?\s*\d+\s*)?employees?',
        r'(\d+)\s*workers?',
        r'company size[:\s]*(\d+)',
    ]
    for pattern in emp_patterns:
        m = re.search(pattern, text_lower)
        if m:
            try:
                count = int(m.group(1))
                if 1 <= count <= 10000:
                    result["employee_count"] = count
                    break
            except ValueError:
                pass

    # Business still active? Look for recent reviews or "open" status
    current_year = datetime.now().year
    if re.search(rf'\b{current_year}\b.*review', text_lower) or \
       re.search(r'open\s*(?:now|today|hours)', text_lower) or \
       re.search(rf'reviews?\s*.*\b{current_year}\b', text_lower):
        result["business_still_active"] = 1

    # Also check for "permanently closed" as counter-signal
    if "permanently closed" in text_lower:
        result["business_still_active"] = 0

    # Website — extract non-directory URLs
    directory_domains = [
        "yelp.com", "yellowpages.com", "bbb.org", "angi.com",
        "homeadvisor.com", "facebook.com", "linkedin.com", "mapquest.com",
        "manta.com", "google.com", "nextdoor.com", "thumbtack.com",
        "whitepages.com", "superpages.com", "dexknows.com",
    ]
    urls = re.findall(r'https?://[^\s"<>]+', html)
    for u in urls:
        u_lower = u.lower()
        if not any(d in u_lower for d in directory_domains):
            if any(ext in u_lower for ext in [".com", ".net", ".org", ".biz"]):
                # Probably a business website
                result["business_website"] = u.split("&")[0][:200]
                result["has_website"] = 1
                break

    # Business type — what kind of work
    type_keywords = {
        "residential plumbing": ["residential plumb", "home plumb"],
        "commercial plumbing": ["commercial plumb"],
        "residential electrical": ["residential electric", "home electric"],
        "commercial electrical": ["commercial electric"],
        "HVAC": ["hvac", "heating", "air conditioning", "cooling"],
        "general plumbing": ["plumbing service", "plumber", "drain", "sewer", "water heater"],
        "general electrical": ["electrical service", "electrician", "wiring", "panel"],
    }
    found_types = []
    for btype, keywords in type_keywords.items():
        for kw in keywords:
            if kw in text_lower:
                found_types.append(btype)
                break
    if found_types:
        result["business_type"] = ", ".join(found_types[:3])

    return result


def parse_contact_search(html, name, city):
    """Parse Google results for contact/LinkedIn info."""
    result = {
        "linkedin_url": None,
        "personal_phone": None,
        "extra_family": [],
    }
    if not html:
        return result

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # LinkedIn URL
    li_match = re.search(r'https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-]+', html)
    if li_match:
        result["linkedin_url"] = li_match.group()

    # Phone numbers
    phones = extract_phone_numbers(text)
    if phones:
        result["personal_phone"] = phones[0]

    # Family from whitepages-style results
    result["extra_family"] = extract_family_names(text, name)

    return result


def run_pass2(page, contractors, conn):
    """Pass 2: Three Google searches per person."""
    print("\n" + "=" * 60)
    print("PASS 2: Google Searches (3 per person)")
    print("=" * 60)

    captcha_hit = False

    for i, c in enumerate(contractors, 1):
        if captcha_hit:
            break

        name = c["name"] or "Unknown"
        city = c["city"] or ""
        biz_name = c.get("business_name") or ""
        license_type = c.get("license_type") or ""
        industry_kw = "plumbing" if "plumb" in license_type.lower() else "electrical"

        # Skip already enriched
        if c.get("deep_checked") or c.get("obituary_found"):
            print(f"\n[{i}/{len(contractors)}] {name} — already enriched, skipping")
            continue

        print(f"\n[{i}/{len(contractors)}] {name} — {city}, NJ")

        # ── Search 1: Obituary ──
        query1 = f'"{name}" "{city}" NJ obituary'
        print(f"  Search 1: obituary")
        html1, captcha = google_search(page, query1)
        if captcha:
            print("  *** CAPTCHA DETECTED — pausing 60s ***")
            time.sleep(60)
            html1, captcha = google_search(page, query1)
            if captcha:
                print("  *** STILL BLOCKED — stopping Google searches ***")
                captcha_hit = True
                _save_progress(conn, c, {}, {}, {})
                continue

        obit_data = parse_obituary_search(html1, name, city)
        if obit_data["obituary_found"]:
            print(f"    Obituary found! Death year: {obit_data['death_year'] or 'unknown'}")
            if obit_data["family_contacts"]:
                print(f"    Family: {', '.join(obit_data['family_contacts'])}")

        time.sleep(random.uniform(12, 18))

        # ── Search 2: Business info ──
        name_part = f'"{name}"'
        biz_part = f' OR "{biz_name}"' if biz_name else ""
        query2 = f'{name_part}{biz_part} {industry_kw} {city} NJ'
        print(f"  Search 2: business info")
        html2, captcha = google_search(page, query2)
        if captcha:
            print("  *** CAPTCHA DETECTED — pausing 60s ***")
            time.sleep(60)
            html2, captcha = google_search(page, query2)
            if captcha:
                print("  *** STILL BLOCKED — stopping Google searches ***")
                captcha_hit = True
                _save_progress(conn, c, obit_data, {}, {})
                continue

        biz_data = parse_business_search(html2, name, city)
        if biz_data["business_phone"]:
            print(f"    Phone: {biz_data['business_phone']}")
        if biz_data["google_rating"]:
            print(f"    Rating: {biz_data['google_rating']} ({biz_data['review_count'] or '?'} reviews)")
        if biz_data["business_still_active"]:
            print(f"    *** BUSINESS STILL ACTIVE ***")
        if biz_data["business_type"]:
            print(f"    Type: {biz_data['business_type']}")

        time.sleep(random.uniform(12, 18))

        # ── Search 3: Contact/LinkedIn ──
        query3 = f'"{name}" {city} NJ linkedin OR phone OR contact'
        print(f"  Search 3: contact info")
        html3, captcha = google_search(page, query3)
        if captcha:
            print("  *** CAPTCHA DETECTED — pausing 60s ***")
            time.sleep(60)
            html3, captcha = google_search(page, query3)
            if captcha:
                print("  *** STILL BLOCKED — stopping Google searches ***")
                captcha_hit = True
                _save_progress(conn, c, obit_data, biz_data, {})
                continue

        contact_data = parse_contact_search(html3, name, city)
        if contact_data["linkedin_url"]:
            print(f"    LinkedIn: {contact_data['linkedin_url']}")
        if contact_data["personal_phone"]:
            print(f"    Personal phone: {contact_data['personal_phone']}")

        # ── Cross-check & save ──
        _save_progress(conn, c, obit_data, biz_data, contact_data)

        time.sleep(random.uniform(12, 18))

    if captcha_hit:
        print("\n*** Google CAPTCHA stopped further searches. Progress saved. ***")

    print(f"\nPass 2 complete.")


def _save_progress(conn, contractor, obit_data, biz_data, contact_data):
    """Save all enrichment data for one contractor to the database."""
    c = contractor
    status = (c.get("status") or "").lower()

    # Merge family contacts from obituary + contact search
    family = list(set(
        obit_data.get("family_contacts", []) +
        contact_data.get("extra_family", [])
    ))

    # Cross-check logic
    business_still_active = biz_data.get("business_still_active", 0)
    verified = 0
    needs_review = 0

    if status == "deceased" and business_still_active:
        # Owner deceased but business still active — potential acquisition
        needs_review = 1
    elif status == "deceased" and obit_data.get("obituary_found"):
        verified = 1
    elif status == "retired" and not business_still_active:
        verified = 1

    # Build enrichment notes
    notes_parts = []
    if obit_data.get("obit_snippet"):
        notes_parts.append(f"Obit: {obit_data['obit_snippet'][:200]}")
    if business_still_active:
        notes_parts.append("BUSINESS STILL ACTIVE — potential acquisition target")
    enrichment_notes = c.get("enrichment_notes") or ""
    if notes_parts:
        enrichment_notes = (enrichment_notes + " | " if enrichment_notes else "") + " | ".join(notes_parts)

    # Use business phone as primary phone if we don't have one
    phone = biz_data.get("business_phone") or contact_data.get("personal_phone")

    conn.execute("""
        UPDATE contractors SET
            issue_date = COALESCE(?, issue_date),
            business_phone = COALESCE(?, business_phone),
            business_address = COALESCE(?, business_address),
            business_website = COALESCE(?, business_website),
            business_type = COALESCE(?, business_type),
            employee_count = COALESCE(?, employee_count),
            death_year = COALESCE(?, death_year),
            obituary_found = MAX(COALESCE(obituary_found, 0), ?),
            family_contacts = COALESCE(?, family_contacts),
            linkedin_url = COALESCE(?, linkedin_url),
            personal_phone = COALESCE(?, personal_phone),
            business_still_active = MAX(COALESCE(business_still_active, 0), ?),
            verified = MAX(COALESCE(verified, 0), ?),
            needs_review = MAX(COALESCE(needs_review, 0), ?),
            enrichment_notes = ?,
            google_rating = COALESCE(?, google_rating),
            review_count = COALESCE(?, review_count),
            has_website = MAX(COALESCE(has_website, 0), ?),
            phone = COALESCE(?, phone),
            last_updated = ?
        WHERE id = ?
    """, (
        c.get("issue_date"),
        biz_data.get("business_phone"),
        biz_data.get("business_address"),
        biz_data.get("business_website"),
        biz_data.get("business_type"),
        biz_data.get("employee_count"),
        obit_data.get("death_year"),
        obit_data.get("obituary_found", 0),
        ", ".join(family) if family else None,
        contact_data.get("linkedin_url"),
        contact_data.get("personal_phone"),
        business_still_active,
        verified,
        needs_review,
        enrichment_notes[:1000] if enrichment_notes else None,
        biz_data.get("google_rating"),
        biz_data.get("review_count"),
        biz_data.get("has_website", 0),
        phone,
        datetime.now().strftime("%Y-%m-%d"),
        c["id"],
    ))
    conn.commit()


# ═══════════════════════════════════════════════════════════════════════════
# Results & Summary
# ═══════════════════════════════════════════════════════════════════════════

def generate_results(conn):
    """Generate sorted results file and print summary."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM contractors
        WHERE status IN ('Deceased', 'Retired')
        ORDER BY
            business_still_active DESC,
            death_year DESC,
            CASE status WHEN 'Deceased' THEN 1 WHEN 'Retired' THEN 2 END,
            name
    """).fetchall()
    contractors = [dict(r) for r in rows]

    os.makedirs(RESULTS_DIR, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(RESULTS_DIR, f"priority_leads_{today}.txt")

    with open(filepath, "w") as f:
        f.write(f"Priority Leads Report — {today}\n")
        f.write(f"Deceased & Retired NJ Master Plumbers + Electrical Contractors\n")
        f.write("=" * 70 + "\n\n")

        for i, c in enumerate(contractors, 1):
            f.write(f"{i}. {c['name']} — {c['city']}, NJ  [{c['status']}]\n")
            f.write(f"   License: {c['license_type']} #{c['license_number']}\n")

            if c.get("business_name"):
                f.write(f"   Business: {c['business_name']}\n")
            if c.get("business_type"):
                f.write(f"   Type: {c['business_type']}\n")
            if c.get("business_phone"):
                f.write(f"   Phone: {c['business_phone']}\n")
            if c.get("business_address"):
                f.write(f"   Address: {c['business_address']}\n")
            if c.get("business_website"):
                f.write(f"   Website: {c['business_website']}\n")
            if c.get("google_rating"):
                f.write(f"   Rating: {c['google_rating']} ({c.get('review_count') or '?'} reviews)\n")
            if c.get("employee_count"):
                f.write(f"   Employees: {c['employee_count']}\n")
            if c.get("death_year"):
                f.write(f"   Death year: {c['death_year']}\n")
            if c.get("obituary_found"):
                f.write(f"   Obituary: Found\n")
            if c.get("family_contacts"):
                f.write(f"   Family: {c['family_contacts']}\n")
            if c.get("linkedin_url"):
                f.write(f"   LinkedIn: {c['linkedin_url']}\n")
            if c.get("personal_phone"):
                f.write(f"   Personal phone: {c['personal_phone']}\n")
            if c.get("issue_date"):
                f.write(f"   License issued: {c['issue_date']}\n")
            if c.get("expiration_date"):
                f.write(f"   License expired: {c['expiration_date']}\n")

            flags = []
            if c.get("business_still_active"):
                flags.append("BUSINESS STILL ACTIVE")
            if c.get("verified"):
                flags.append("VERIFIED")
            if c.get("needs_review"):
                flags.append("NEEDS REVIEW")
            if flags:
                f.write(f"   *** {' | '.join(flags)} ***\n")

            if c.get("enrichment_notes"):
                f.write(f"   Notes: {c['enrichment_notes'][:300]}\n")

            f.write("\n")

    print(f"\nResults saved to {filepath}")
    return contractors


def print_summary(contractors):
    """Print the final summary stats."""
    current_year = datetime.now().year
    total = len(contractors)
    deceased = sum(1 for c in contractors if c["status"] == "Deceased")
    retired = sum(1 for c in contractors if c["status"] == "Retired")

    died_2yr = sum(1 for c in contractors
                   if c.get("death_year") and c["death_year"] >= current_year - 2)
    died_5yr = sum(1 for c in contractors
                   if c.get("death_year") and c["death_year"] >= current_year - 5)
    biz_active = sum(1 for c in contractors if c.get("business_still_active"))
    with_obit = sum(1 for c in contractors if c.get("obituary_found"))
    with_phone = sum(1 for c in contractors if c.get("business_phone") or c.get("personal_phone"))
    with_linkedin = sum(1 for c in contractors if c.get("linkedin_url"))
    verified = sum(1 for c in contractors if c.get("verified"))
    needs_review = sum(1 for c in contractors if c.get("needs_review"))

    print("\n" + "=" * 60)
    print("ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"  Total enriched:               {total}")
    print(f"    Deceased:                    {deceased}")
    print(f"    Retired:                     {retired}")
    print(f"  Died in last 2 years (2024+):  {died_2yr}")
    print(f"  Died in last 5 years (2021+):  {died_5yr}")
    print(f"  Business still active:         {biz_active}")
    print(f"  Obituary found:                {with_obit}")
    print(f"  Has phone number:              {with_phone}")
    print(f"  Has LinkedIn:                  {with_linkedin}")
    print(f"  Verified:                      {verified}")
    print(f"  Needs review:                  {needs_review}")

    # Top 20 most actionable
    # Score: business_still_active (huge), recent death, has phone, has family
    def actionability(c):
        score = 0
        if c.get("business_still_active"):
            score += 100
        if c.get("death_year"):
            recency = max(0, 10 - (current_year - c["death_year"]))
            score += recency * 5
        if c.get("business_phone") or c.get("personal_phone"):
            score += 20
        if c.get("family_contacts"):
            score += 15
        if c.get("google_rating"):
            score += 10
        if c.get("obituary_found"):
            score += 5
        if c.get("linkedin_url"):
            score += 5
        return score

    ranked = sorted(contractors, key=actionability, reverse=True)
    top20 = ranked[:20]

    print(f"\nTOP 20 MOST ACTIONABLE LEADS:")
    print("-" * 60)
    for i, c in enumerate(top20, 1):
        name = c["name"]
        city = c["city"] or ""
        status = c["status"]
        phone = c.get("business_phone") or c.get("personal_phone") or "no phone"
        dy = c.get("death_year") or ""
        active = " [BIZ ACTIVE]" if c.get("business_still_active") else ""
        btype = c.get("business_type") or ""

        print(f"  {i:2d}. {name} — {city}, NJ  ({status}{' ' + str(dy) if dy else ''}){active}")
        if btype:
            print(f"      Type: {btype}")
        print(f"      Phone: {phone}")
        if c.get("family_contacts"):
            print(f"      Family: {c['family_contacts']}")
        if c.get("business_name"):
            print(f"      Business: {c['business_name']}")

    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("ENRICH PRIORITY LEADS")
    print(f"Deceased & Retired NJ Contractors")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    add_columns(conn)

    contractors = get_priority_contractors(conn)
    deceased = sum(1 for c in contractors if c["status"] == "Deceased")
    retired = sum(1 for c in contractors if c["status"] == "Retired")
    print(f"\nLoaded {len(contractors)} priority contractors")
    print(f"  {deceased} Deceased + {retired} Retired")

    with sync_playwright() as p:
        browser, context, page = launch_browser(p)

        try:
            # Pass 1: Detail pages
            run_pass1(page, contractors, conn)

            # Reload data with Pass 1 updates before Pass 2
            contractors = get_priority_contractors(conn)

            # Pass 2: Google searches
            run_pass2(page, contractors, conn)

        except KeyboardInterrupt:
            print("\n\n*** Interrupted — saving progress ***")
        except Exception as e:
            print(f"\n[FATAL ERROR] {e}")
            import traceback
            traceback.print_exc()
        finally:
            browser.close()

    # Reload final data and generate report
    contractors = get_priority_contractors(conn)
    contractors_enriched = generate_results(conn)
    print_summary(contractors_enriched)

    conn.close()
    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
