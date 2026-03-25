# Acquisition Edge

Find plumbing and electrical companies likely to sell — before they list.

Builds a database of every licensed contractor in the tri-state area (NY/NJ/CT), then monitors for distress signals: lapsing licenses, court filings, declining reviews, and more.

## What It Does

1. Downloads every licensed plumber and electrician from state databases
2. Stores them in a local SQLite database
3. Scores each on distress signals (expired license, court filings, bad reviews)
4. Surfaces the highest-probability acquisition targets
5. Generates one-page dossiers for outreach

## Setup

### Requirements
- Python 3.8+
- pip

### Install
```bash
cd acquisition-edge
pip install -r requirements.txt
python -m playwright install chromium
```

### Run
```bash
python3 main.py                    # Full run (all states)
python3 main.py --nj-only          # New Jersey only
python3 main.py --nyc-only         # NYC only
```

## How It's Different From Public Deal Flow

**Public Deal Flow** finds businesses already listed for sale on BizBuySell etc. Every buyer sees those.

**Acquisition Edge** finds businesses about to become available by monitoring public records nobody else is watching systematically.
