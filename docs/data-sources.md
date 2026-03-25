# Data Sources — Acquisition Edge

## Active (Phase 1)

### New Jersey — MyLicense Bulk Download
- **URL:** https://newjersey.mylicense.com/Verification_Bulk/Search.aspx?facility=N
- **What:** Statewide license database for all professions
- **License types we pull:**
  - Master Plumber, Journeyman Plumber, Apprentice Plumber
  - Electrical Contractor, Class A Journeyman Electrician, Class B Wireman
- **Data:** Name, license number, license type, status, expiration date, city
- **CAPTCHA:** None
- **Scraping feasibility:** HIGH — they offer bulk download
- **Refresh frequency:** Weekly

### NYC — Department of Buildings (BIS Web)
- **URL:** https://a810-bisweb.nyc.gov/bisweb/LicenseTypeServlet
- **What:** NYC-licensed Master Plumbers and Master Electricians
- **Data:** Name, license number, status, insurance status
- **CAPTCHA:** None
- **Scraping feasibility:** Moderate — old servlet, can be slow
- **Refresh frequency:** Weekly

## Planned (Phase 2)

### Connecticut — eLicense
- **URL:** https://www.elicense.ct.gov/Lookup/LicenseLookup.aspx
- **Blocker:** CAPTCHA on search. Need workaround or direct data request.
- **Contact:** dcp.online@ct.gov

### Google Reviews (tracking over time)
- Search each contractor on Google Maps
- Store rating + review count weekly
- Flag declining trends

### NJ/NY Court Dockets
- Divorce and probate filings
- Cross-reference contractor names
- Public record, varies by county

### Secretary of State Business Filings
- NJ: https://www.njportal.com/DOR/BusinessNameSearch
- NY: https://apps.dos.ny.gov/publicInquiry/
- Flag delinquent annual filings
