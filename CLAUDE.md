# CLAUDE.md — Acquisition Edge

> Read this every session.

## What This Is

Acquisition intelligence system that finds plumbing/electrical companies likely to sell BEFORE they list. Builds a database of every licensed contractor in the tri-state area, then monitors for distress signals (lapsing licenses, court filings, declining reviews).

## Owner

- **Mike** — Not an engineer. Explain simply. Push back on bad ideas. Keep it working.

## Quick Reference

- **Run:** `python3 main.py`
- **Config:** `config.json`
- **Database:** `data/contractors.db` (SQLite)
- **Entry point:** `main.py`

## Rules

- Test before pushing. Always.
- Keep it simple. One command to run.
- Update docs when files change.
- Never commit secrets (.env, service-account.json).

## Documentation

| Doc | Purpose | Update when... |
|---|---|---|
| `docs/architecture.md` | File map, how it works | Files added/removed |
| `docs/vision.md` | Product roadmap | Direction changes |
| `docs/data-sources.md` | What we scrape and how | Sources added/removed |
| `CHANGELOG.md` | What shipped | Every session |
| `README.md` | Overview + setup | Major changes |
