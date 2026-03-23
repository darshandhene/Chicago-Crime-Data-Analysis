# Chicago Crime & 311 Analytics Pipeline

A production-grade 3-layer ETL pipeline analyzing 260,000+ Chicago crime incidents 
and 50,000 311 service requests from the City of Chicago Open Data Portal.

## Architecture
Raw → Staging → Curated (Medallion-style)

## Tech Stack
Python, DuckDB, SQL, Tableau Public

## Pipeline Design
- **Raw layer:** Ingests CSV data directly via DuckDB's `read_csv_auto`
- **Staging layer:** Cleans, casts, and deduplicates using `QUALIFY ROW_NUMBER()`
- **Curated layer:** Star schema with fact and dimension tables
- **Run manifest:** Tracks row counts per layer for reproducibility and auditability

## Data Quality
- Cardinality checks (pre/post join row count validation)
- Null and duplicate audits on all key columns
- Schema contracts enforced at each layer
- Parquet export for downstream analytics

## Key Findings (via Tableau Dashboard)
- Theft is the most common crime; Weapons Violations have the highest arrest rate (~40%)
- Friday and Saturday peak crime days; summer months drive highest volume
- Chicago Lawn, Near West, and Gresham are highest-crime districts
- Streets and apartments account for 65% of all crimes
- 311 backlog shows weak negative correlation with daily crime (r = -0.19)

## How to Run
```bash
# Download raw data into data/raw/
python3 build.py
```

## Dashboard
[View on Tableau Public] https://public.tableau.com/app/profile/darshan.sachin.dhene/viz/ChicagoCrime311AnalyticsDashboard/Dashboard1?publish=yes
