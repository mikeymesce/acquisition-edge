"""
Two-score ranking engine for Acquisition Edge.

Business Quality Score (0-100): How good is this business to acquire?
Seller Motivation Score (0-100): How likely is the owner to sell?
Combined Score = (Quality * 0.6) + (Motivation * 0.4)
"""

import json
import sqlite3
import os
from datetime import datetime, timedelta


DB_PATH = os.path.join(os.path.dirname(__file__), "data", "contractors.db")


def load_config():
    """Load scoring weights from config.json."""
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path, "r") as f:
        return json.load(f)


def _add_score_columns(conn):
    """Add quality_score, motivation_score, combined_score columns if missing."""
    new_cols = [
        ("quality_score", "INTEGER DEFAULT 0"),
        ("motivation_score", "INTEGER DEFAULT 0"),
        ("combined_score", "REAL DEFAULT 0"),
    ]
    for col_name, col_type in new_cols:
        try:
            conn.execute(f"ALTER TABLE contractors ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()


def _calc_quality_score(row):
    """Business Quality Score (0-100): How attractive is this business?"""
    score = 0
    notes = []

    # Active license: 15pts (expired = 0)
    status = (row["status"] or "").lower()
    if "active" in status:
        score += 15
        notes.append("Active license (+15)")

    # Has website: 15pts
    has_website = row.get("has_website") or 0
    if has_website:
        score += 15
        notes.append("Has website (+15)")

    # Google rating
    rating = row.get("google_rating")
    if rating is not None:
        if rating >= 4.0:
            score += 20
            notes.append(f"Rating {rating} (+20)")
        elif rating >= 3.5:
            score += 10
            notes.append(f"Rating {rating} (+10)")

    # Review count
    reviews = row.get("review_count")
    if reviews is not None:
        if reviews >= 100:
            score += 20
            notes.append(f"{reviews} reviews (+20)")
        elif reviews >= 50:
            score += 15
            notes.append(f"{reviews} reviews (+15)")
        elif reviews >= 10:
            score += 10
            notes.append(f"{reviews} reviews (+10)")
        else:
            score += 5
            notes.append(f"{reviews} reviews (+5)")

    # Cap at 100
    score = min(score, 100)
    return score, notes


def _calc_motivation_score(row):
    """Seller Motivation Score (0-100): How likely is the owner to sell?"""
    score = 0
    notes = []

    status = (row["status"] or "").lower()
    signals_str = (row.get("distress_signals") or "").lower()
    signals = [s.strip() for s in signals_str.split(",") if s.strip()]
    notes_existing = (row.get("notes") or "").lower()

    # Combine all text to search for keywords
    all_text = f"{signals_str} {notes_existing}"

    # Owner died / obituary / estate / probate: 30pts
    death_keywords = ["obituary", "passed away", "died", "estate", "probate"]
    if any(kw in all_text for kw in death_keywords):
        score += 30
        notes.append("Death/estate signal (+30)")

    # Divorce: 25pts
    if "divorce" in all_text:
        score += 25
        notes.append("Divorce signal (+25)")

    # License expired: 20pts
    if "expired" in status or "lapsed" in status:
        score += 20
        notes.append("License expired (+20)")

    # Retiring/retired: 20pts
    if "retired" in all_text or "retiring" in all_text:
        score += 20
        notes.append("Retiring (+20)")

    # Out of business / permanently closed / for sale: 20pts
    closed_keywords = ["out of business", "permanently closed", "for sale", "shutting down", "closed"]
    if any(kw in all_text for kw in closed_keywords):
        score += 20
        notes.append("Closed/for sale (+20)")

    # License revoked/suspended: 15pts
    if "revoked" in status or "suspended" in status:
        score += 15
        notes.append(f"License {status} (+15)")

    # No website + active license (neglect signal): 5pts
    has_website = row.get("has_website") or 0
    if not has_website and "active" in status:
        score += 5
        notes.append("No website but active (+5)")

    # Cap at 100
    score = min(score, 100)
    return score, notes


def score_all():
    """Score every contractor in the database with the two-score model."""
    if not os.path.exists(DB_PATH):
        print("No database found. Run scrapers first.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _add_score_columns(conn)

    cursor = conn.execute("SELECT * FROM contractors")
    rows = cursor.fetchall()

    print(f"Scoring {len(rows)} contractors (two-score model)...")
    quality_hits = 0
    motivation_hits = 0

    for row in rows:
        row_dict = dict(row)

        quality, q_notes = _calc_quality_score(row_dict)
        motivation, m_notes = _calc_motivation_score(row_dict)
        combined = (quality * 0.6) + (motivation * 0.4)

        # Also keep the old distress_score updated for backwards compat
        all_notes = q_notes + m_notes
        notes_str = "; ".join(all_notes) if all_notes else None

        conn.execute("""
            UPDATE contractors SET
                quality_score = ?,
                motivation_score = ?,
                combined_score = ?,
                distress_score = ?,
                notes = ?
            WHERE id = ?
        """, (quality, motivation, round(combined, 1), motivation, notes_str, row["id"]))

        if quality > 0:
            quality_hits += 1
        if motivation > 0:
            motivation_hits += 1

    conn.commit()
    conn.close()

    print(f"  Quality signals: {quality_hits} contractors")
    print(f"  Motivation signals: {motivation_hits} contractors")
    return motivation_hits


def get_top_leads(limit=10):
    """Return the top-scored contractors by combined score."""
    if not os.path.exists(DB_PATH):
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Try combined_score first, fall back to distress_score
    try:
        cursor = conn.execute("""
            SELECT * FROM contractors
            WHERE combined_score > 0
            ORDER BY combined_score DESC
            LIMIT ?
        """, (limit,))
    except sqlite3.OperationalError:
        cursor = conn.execute("""
            SELECT * FROM contractors
            WHERE distress_score > 0
            ORDER BY distress_score DESC
            LIMIT ?
        """, (limit,))

    leads = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return leads
