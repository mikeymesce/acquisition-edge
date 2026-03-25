# Vision — Acquisition Edge

## Goal

Find plumbing and electrical companies in the tri-state area (NY/NJ/CT) that are likely to sell — BEFORE they list on any marketplace. Surface 1-2 high-probability targets per week with full dossiers.

## Target Profile

- **Industry:** Plumbing, Electrical
- **Employees:** 4–40
- **Profit (SDE):** $100K–$1M
- **Location:** New York, New Jersey, Connecticut
- **Key signals:** Lapsing licenses, court filings (divorce/probate), declining reviews, owner age

## How It's Different From Public Deal Flow

Public Deal Flow finds businesses **already listed for sale** on marketplaces. Everyone sees those.

Acquisition Edge finds businesses **about to become available** by monitoring public records. Nobody else is watching these signals systematically. That's the edge.

## Phases

### Phase 1 (current) — License Database
- Download every licensed plumber and electrician in NJ (bulk download tool)
- Scrape NYC licensed contractors from BIS Web
- Store in SQLite database
- Flag: expired licenses, lapsing licenses, disciplinary actions

### Phase 2 — Distress Signals
- Monitor Google Reviews over time (declining = business in trouble)
- Track Secretary of State filings (delinquent = dissolving)
- Cross-reference contractor names with court dockets (divorce/probate)

### Phase 3 — Dossier Generator
- For high-scoring companies, auto-compile a one-pager:
  - Owner name, license status, business age
  - Google rating trend
  - Court filing matches
  - Estimated revenue (from employee count + industry benchmarks)
  - Recommended outreach approach

### Phase 4 — Outreach
- Auto-generate personalized letters
- Track contacts and follow-ups
- CRM-lite functionality
