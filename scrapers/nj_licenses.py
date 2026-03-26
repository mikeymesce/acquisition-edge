"""
New Jersey License Scraper

Downloads all licensed plumbers and electricians from the NJ Division of
Consumer Affairs MyLicense portal.

Source: https://newjersey.mylicense.com/verification/Search.aspx

The results page uses a table with id="datagrid_results". Each row has:
  - Full Name (nested in a sub-table with a link to details page)
  - License Number
  - Profession
  - License Type
  - License Status (Active, Expired, etc.)
  - City
  - State
"""

import os
import re
import time
import sqlite3
import random
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "contractors.db")

# Exact dropdown values from the NJ portal
LICENSE_SEARCHES = [
    {
        "profession": "Master Plumbers",
        "license_type": "Master Plumber",
        "industry": "Plumbing",
    },
    {
        "profession": "Electrical Contractors",
        "license_type": "Electrical Contractor",
        "industry": "Electrical",
    },
]


def _ensure_db():
    """Create the database and contractors table if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS contractors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            business_name TEXT,
            license_number TEXT UNIQUE,
            license_type TEXT,
            status TEXT,
            expiration_date TEXT,
            state TEXT DEFAULT 'NJ',
            city TEXT,
            phone TEXT,
            website TEXT,
            first_seen TEXT,
            last_updated TEXT,
            distress_score INTEGER DEFAULT 0,
            notes TEXT,
            source TEXT DEFAULT 'NJ MyLicense',
            detail_url TEXT,
            industry TEXT
        )
    """)
    conn.commit()
    return conn


def _launch_browser(playwright):
    """Launch Chromium with anti-detection settings."""
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


def _polite_delay(min_sec=2, max_sec=4):
    time.sleep(random.uniform(min_sec, max_sec))


def _get_page_content_with_retry(page, max_retries=3):
    """Get page content with retry logic."""
    for attempt in range(max_retries):
        try:
            return page.content()
        except Exception as e:
            print(f"    [RETRY {attempt + 1}/{max_retries}] page.content() failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
    return ""


def scrape_nj_licenses():
    """Scrape NJ MyLicense for plumbing and electrical licenses."""
    print("=" * 50)
    print("NJ License Scraper")
    print("=" * 50)

    all_contractors = []

    with sync_playwright() as p:
        browser, context, page = _launch_browser(p)

        for search in LICENSE_SEARCHES:
            print(f"\nSearching: {search['profession']} — {search['license_type']}")
            _polite_delay(2, 4)

            contractors = _search_and_parse(page, search)
            all_contractors.extend(contractors)
            print(f"  Total for {search['license_type']}: {len(contractors)}")

        browser.close()

    print(f"\nTotal NJ contractors found: {len(all_contractors)}")
    return all_contractors


def _search_and_parse(page, search):
    """Navigate to search page, fill form, parse all pages of results."""
    contractors = []
    url = "https://newjersey.mylicense.com/verification/Search.aspx"

    try:
        page.goto(url, timeout=30000, wait_until="networkidle")
        page.wait_for_timeout(2000)
    except Exception as e:
        print(f"  [ERROR] Failed to load page: {e}")
        return contractors

    try:
        # Select profession
        page.select_option("#t_web_lookup__profession_name", value=search["profession"])
        page.wait_for_timeout(3000)  # Wait for license types to load

        # Select license type
        page.select_option("#t_web_lookup__license_type_name", value=search["license_type"])
        page.wait_for_timeout(1000)

        # Click search
        btn = page.query_selector('input[type="submit"][value="Search"], #sch_button')
        if btn:
            btn.click()
        else:
            page.keyboard.press("Enter")
        page.wait_for_timeout(5000)

        # Try to figure out expected page count from pager row
        try:
            html = _get_page_content_with_retry(page)
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", id="datagrid_results")
            if table:
                # Find the last page link in the pager row
                pager_links = table.find_all("a", href=True)
                page_numbers = []
                for link in pager_links:
                    txt = link.get_text(strip=True)
                    if txt.isdigit():
                        page_numbers.append(int(txt))
                if page_numbers:
                    print(f"  Expected pages: at least {max(page_numbers)}+")
        except Exception:
            pass

        # Parse first page
        page_results = _parse_datagrid(page, search)
        contractors.extend(page_results)
        print(f"  Page 1: {len(page_results)} results")

        # Incremental save after first page
        if page_results:
            save_to_db(page_results)

        # Follow pagination — NJ uses numbered page links via __doPostBack
        page_num = 1
        while True:
            # Find the highest numbered page link available
            next_page = page_num + 1
            next_link = page.query_selector(
                f'#datagrid_results a:has-text("{next_page}")'
            )

            if not next_link:
                # No more pages
                break

            page_num = next_page
            if page_num > 500:  # Safety limit
                print(f"  Hit page limit (500)")
                break

            _polite_delay(1, 2)

            # Click with retry logic
            click_success = False
            for attempt in range(3):
                try:
                    next_link.click()
                    # Wait for the datagrid to re-attach after postback
                    page.wait_for_selector('#datagrid_results', state='attached', timeout=15000)
                    page.wait_for_timeout(3000)
                    click_success = True
                    break
                except Exception as e:
                    print(f"    [RETRY {attempt + 1}/3] Pagination click failed: {e}")
                    if attempt < 2:
                        time.sleep(5)
                        # Re-find the link after waiting
                        next_link = page.query_selector(
                            f'#datagrid_results a:has-text("{next_page}")'
                        )
                        if not next_link:
                            break

            if not click_success:
                print(f"  [ERROR] Could not navigate to page {next_page}. Stopping pagination.")
                break

            page_results = _parse_datagrid(page, search)
            if not page_results:
                break

            contractors.extend(page_results)

            # Incremental save after EACH page
            save_to_db(page_results)

            if page_num % 10 == 0:
                print(f"  Page {page_num}: {len(contractors)} total so far")

    except Exception as e:
        print(f"  [ERROR] Search failed: {e}")

    return contractors


def _parse_datagrid(page, search):
    """Parse the datagrid_results table on the current page."""
    html = _get_page_content_with_retry(page)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", id="datagrid_results")
    if not table:
        return []

    contractors = []
    # Rows are inside a <tbody>, so don't use recursive=False
    all_rows = table.find_all("tr")

    # Skip header row and nested sub-table rows
    for row in all_rows[1:]:
        cells = row.find_all("td", recursive=False)
        if len(cells) < 5:
            continue

        # Column order: Name, License#, Profession, LicenseType, Status, City, State
        # Name cell contains a nested table with a link
        name_link = cells[0].find("a")
        name = name_link.get_text(strip=True) if name_link else ""
        detail_href = name_link.get("href", "") if name_link else ""

        if detail_href and not detail_href.startswith("http"):
            detail_href = "https://newjersey.mylicense.com/verification/" + detail_href

        # Extract spans from remaining cells
        def cell_text(idx):
            if idx < len(cells):
                span = cells[idx].find("span")
                if span:
                    return span.get_text(strip=True)
                return cells[idx].get_text(strip=True)
            return ""

        license_number = cell_text(1)
        profession = cell_text(2)
        license_type = cell_text(3)
        status = cell_text(4)
        city = cell_text(5)
        state = cell_text(6)

        # Skip if no name or looks like a pager/header row
        if not name or name.isdigit():
            continue

        contractors.append({
            "name": name,
            "business_name": "",  # Not on list page, would need detail page
            "license_number": license_number,
            "license_type": license_type or search["license_type"],
            "status": status,
            "expiration_date": "",  # Not on list page, would need detail page
            "state": state or "NJ",
            "city": city,
            "source": "NJ MyLicense",
            "detail_url": detail_href,
            "industry": search["industry"],
        })

    return contractors


def save_to_db(contractors):
    """Save contractors to SQLite. Updates existing, inserts new."""
    conn = _ensure_db()
    today = datetime.now().strftime("%Y-%m-%d")
    inserted = 0
    updated = 0

    for c in contractors:
        if c.get("license_number"):
            cursor = conn.execute(
                "SELECT id FROM contractors WHERE license_number = ?",
                (c["license_number"],)
            )
            existing = cursor.fetchone()

            if existing:
                conn.execute("""
                    UPDATE contractors SET
                        name = ?, business_name = ?, license_type = ?,
                        status = ?, expiration_date = ?, city = ?,
                        last_updated = ?, source = ?, detail_url = ?,
                        industry = ?
                    WHERE license_number = ?
                """, (
                    c["name"], c["business_name"], c["license_type"],
                    c["status"], c["expiration_date"], c["city"],
                    today, c["source"], c.get("detail_url", ""),
                    c.get("industry", ""), c["license_number"]
                ))
                updated += 1
                continue

        try:
            conn.execute("""
                INSERT INTO contractors
                    (name, business_name, license_number, license_type,
                     status, expiration_date, state, city, first_seen,
                     last_updated, source, detail_url, industry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                c["name"], c["business_name"], c["license_number"],
                c["license_type"], c["status"], c["expiration_date"],
                c["state"], c["city"], today, today, c["source"],
                c.get("detail_url", ""), c.get("industry", "")
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            updated += 1

    conn.commit()
    conn.close()

    print(f"Database: {inserted} new, {updated} updated")
    return inserted, updated
