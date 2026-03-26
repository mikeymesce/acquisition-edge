#!/usr/bin/env python3
"""
Acquisition Edge — Find businesses likely to sell before they list.

Downloads contractor license data, scores on distress signals,
and surfaces top acquisition targets.

Usage:
    python3 main.py              # Full run (all states)
    python3 main.py --nj-only    # New Jersey only
    python3 main.py --nyc-only   # NYC only
"""

import os
import sys
from datetime import datetime

# Run from project directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from scrapers.nj_licenses import scrape_nj_licenses, save_to_db
from scoring import score_all, get_top_leads


def main():
    nj_only = "--nj-only" in sys.argv
    nyc_only = "--nyc-only" in sys.argv

    print(f"{'=' * 50}")
    print(f"Acquisition Edge — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'=' * 50}\n")

    all_contractors = []

    # --- NJ License Scraper ---
    if not nyc_only:
        nj_results = scrape_nj_licenses()
        all_contractors.extend(nj_results)

    # --- NYC License Scraper (Phase 1b — coming soon) ---
    if not nj_only:
        # TODO: NYC BIS Web scraper
        pass

    if not all_contractors:
        print("\nNo contractors found. Check errors above.")
        sys.exit(1)

    # Save to database
    print(f"\nSaving {len(all_contractors)} contractors to database...")
    save_to_db(all_contractors)

    # Score everyone
    print()
    score_all()

    # Show top leads
    top = get_top_leads(limit=10)
    if top:
        print(f"\n{'=' * 50}")
        print(f"Top {len(top)} Leads (by Combined Score):")
        print(f"{'=' * 50}")
        for i, lead in enumerate(top, 1):
            name = lead["name"] or lead["business_name"] or "Unknown"
            city = lead["city"] or "Unknown"
            status = lead["status"] or "Unknown"
            combined = lead.get("combined_score", 0)
            quality = lead.get("quality_score", 0)
            motivation = lead.get("motivation_score", 0)
            notes = lead["notes"] or ""
            print(f"\n  {i}. {name}")
            print(f"     {lead['license_type']} | {city}, NJ | Status: {status}")
            print(f"     Combined: {combined} (Quality: {quality}, Motivation: {motivation})")
            if notes:
                print(f"     Signals: {notes}")
    else:
        print("\nNo leads found in this run.")

    print(f"\n{'=' * 50}")
    print("Done!")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
