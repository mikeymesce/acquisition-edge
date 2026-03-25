# Architecture — Acquisition Edge

## File Map

```
acquisition-edge/
├── main.py                  # Entry point
├── config.json              # Search criteria and settings
├── requirements.txt         # Python dependencies
├── .env                     # Secrets (not in git)
├── .gitignore
├── data/
│   └── contractors.db       # SQLite database of all contractors
├── scrapers/
│   ├── __init__.py
│   ├── nj_licenses.py       # NJ bulk license downloader
│   └── nyc_licenses.py      # NYC BIS Web scraper
├── scoring.py               # Distress scoring engine
├── output.py                # CSV / Google Sheets / email output
├── docs/
│   ├── architecture.md      # This file
│   ├── vision.md            # Roadmap
│   └── data-sources.md      # What we scrape
├── CLAUDE.md
├── CHANGELOG.md
└── README.md
```

## How It Works

1. `main.py` loads config, runs scrapers
2. `scrapers/nj_licenses.py` downloads NJ license data (bulk)
3. `scrapers/nyc_licenses.py` scrapes NYC BIS Web
4. All contractors stored in `data/contractors.db` (SQLite)
5. `scoring.py` scores each contractor on distress signals
6. `output.py` surfaces top results

## Database Schema

### contractors table
| Column | Type | Description |
|---|---|---|
| id | INTEGER | Primary key |
| name | TEXT | Contractor name |
| business_name | TEXT | Business/company name |
| license_number | TEXT | License number |
| license_type | TEXT | Master Plumber, Electrician, etc. |
| status | TEXT | Active, Expired, Revoked, etc. |
| expiration_date | TEXT | License expiration |
| state | TEXT | NJ, NY, CT |
| city | TEXT | City/location |
| phone | TEXT | If available |
| website | TEXT | If available |
| first_seen | TEXT | Date we first found them |
| last_updated | TEXT | Date of last data refresh |
| distress_score | INTEGER | Calculated distress score |
| notes | TEXT | Any flags or signals |

## Data Flow

```
NJ Bulk Download ──┐
                   ├──> contractors.db ──> scoring.py ──> output.py ──> Dossier
NYC BIS Web ───────┘                         ↑
                                    Google Reviews (Phase 2)
                                    Court Dockets (Phase 2)
                                    SOS Filings (Phase 2)
```
