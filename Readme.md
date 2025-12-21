# OLX MDA — Star Schema Preparation

This repository contains a minimal ETL setup (T-SQL) to build a star schema for market data analysis (MDA) from a flat CSV export of OLX housing offers.

Files
- `olx_house_price_Q122.csv` — source CSV with raw OLX offers (quarterly snapshot).
- `OlxSchema.sql` — creates the star schema (dimension and fact tables).
- `OlxData.sql` — transforms and loads data from the raw table into the dimensional model.

High-level overview
- The pipeline expects a staging table named `olx_house_price` containing the CSV data.
- Running `OlxSchema.sql` creates these tables:
	- `DimOffer` — offer type (private/agency).
	- `DimDate` — month/year lookup with YearMonth numeric key.
	- `DimLocation` — city/region with basic population and status (capital/regional/other).
	- `DimMarket` — market label.
	- `DimProperty` — property type, floor, area bucket and rooms bucket.
	- `FactOfferSnapshot` — fact table storing OfferKey, MarketKey, DateKey, LocationKey, PropertyKey, Area, Price.

Key data cleaning / transformation notes
- The dataset contains area values with inconsistent formatting; some values are missing the decimal point (e.g. `4223` instead of `42.23`).
- `OlxData.sql` applies a simple cleaning rule: if `area > 1500` then treat the value as scaled by 100 and divide by 100.0. Values above ~4500 (after interpreting) are considered outliers and require manual attention for production.
- Rooms are bucketed so that 4 or more rooms are labeled `4+`.
- Dates are parsed from textual month names into numeric months to form a `YearMonth` key (e.g. `202203`).

How to use
1) Load the CSV into a staging table called `olx_house_price`.
	 - Easiest: use SQL Server Management Studio (Import Flat File or Import Data wizard) and map columns to a staging table `olx_house_price`.
	 - Example BULK INSERT (adjust the target table schema and path to match your environment):

```sql
USE <YourDatabase>;
-- Create a staging table with appropriate columns first, then:
BULK INSERT dbo.olx_house_price
FROM 'C:\Users\<User>\Documents\source\OlxMda\olx_house_price_Q122.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a',
	CODEPAGE = '65001',
	TABLOCK
);
```

2) Create the schema by running `OlxSchema.sql` (creates dims and fact table definitions).
3) Run `OlxData.sql` to populate dimension tables and insert fact rows from the staging data (it contains the cleaning rules and dimension lookups).

Running the scripts from PowerShell using `sqlcmd` (Windows auth):

```powershell
# Run the schema
sqlcmd -S <serverName> -d <databaseName> -E -i "C:\Users\<User>\Documents\source\OlxMda\OlxSchema.sql"
# Load data / run transforms
sqlcmd -S <serverName> -d <databaseName> -E -i "C:\Users\<User>\Documents\source\OlxMda\OlxData.sql"
```

If you use SQL authentication, replace `-E` with `-U <username> -P <password>`.

Quick validation queries

```sql
-- Expect non-zero counts after the ETL
SELECT COUNT(*) AS Offers FROM FactOfferSnapshot;
SELECT COUNT(*) AS Cities FROM DimLocation;
SELECT TOP (10) Price, Area FROM FactOfferSnapshot ORDER BY Price DESC;
```

Assumptions & caveats
- The ETL in `OlxData.sql` uses a pragmatic rule to fix malformed `area` values; this is acceptable for an MVP but should be audited for production data.
- The scripts assume `olx_house_price` columns match the CSV layout. If column names or order differ, adapt the staging table or the import step.
- The `DimLocation` script contains a small hard-coded city status map (national/regional capitals). Extend or replace with a reliable geo lookup for production.

Next steps (suggested)
- Add a small validation/test suite (row counts, null-checks, range checks) to verify data after each run.
- Add indexes on foreign keys and commonly filtered columns (DateKey, LocationKey, MarketKey) for query performance.
- Create a documented process for handling outliers and area values that are ambiguous.

Contact / license
- This is a minimal demo for preparing a star schema for MDA. Use and adapt as needed.

---
Generated from repository files: `OlxSchema.sql` and `OlxData.sql`.

