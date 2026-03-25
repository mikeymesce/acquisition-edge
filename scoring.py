"""
Distress scoring engine for Acquisition Edge.

Scores each contractor on likelihood of selling based on:
- License status (expired, expiring soon, disciplinary action)
- Future: court filings, review trends, SOS filings
"""

import json
import sqlite3
import os
from datetime import datetime, timedelta


DB_PATH = os.path.join(os.path.dirname(__file__), "data", "contractors.db")


def load_config():
    """Load scoring weights from config.json."""
    with open("config.json", "r") as f:
        return json.load(f)


def score_all():
    """Score every contractor in the database."""
    if not os.path.exists(DB_PATH):
        print("No database found. Run scrapers first.")
        return

    config = load_config()
    weights = config.get("distress_weights", {})

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute("SELECT * FROM contractors")
    rows = cursor.fetchall()

    print(f"Scoring {len(rows)} contractors...")
    today = datetime.now()
    scored = 0

    for row in rows:
        score = 0
        notes = []

        # --- License status signals ---
        status = (row["status"] or "").lower()

        # Expired license = strong signal
        if "expired" in status or "lapsed" in status:
            score += weights.get("license_expired", 25)
            notes.append("License expired")

        # Revoked or suspended = very strong signal
        if "revoked" in status or "suspended" in status:
            score += weights.get("disciplinary_action", 20)
            notes.append(f"License {status}")

        # License expiring within 90 days
        exp_date_str = row["expiration_date"] or ""
        if exp_date_str:
            try:
                # Try common date formats
                for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"]:
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
                        notes.append(f"License expires in {days_until} days")
                    elif days_until < 0:
                        # Already expired but status might not say so
                        if "expired" not in status:
                            score += weights.get("license_expired", 25)
                            notes.append(f"License expired {abs(days_until)} days ago")
            except Exception:
                pass

        # Update the database
        conn.execute(
            "UPDATE contractors SET distress_score = ?, notes = ? WHERE id = ?",
            (score, "; ".join(notes) if notes else None, row["id"])
        )
        if score > 0:
            scored += 1

    conn.commit()
    conn.close()

    print(f"Scored: {scored} contractors have distress signals")
    return scored


def get_top_leads(limit=10):
    """Return the top-scored contractors."""
    if not os.path.exists(DB_PATH):
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute("""
        SELECT * FROM contractors
        WHERE distress_score > 0
        ORDER BY distress_score DESC
        LIMIT ?
    """, (limit,))

    leads = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return leads
