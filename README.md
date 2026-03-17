# Drug Shortage Dashboard

A data pipeline and dashboard for monitoring FDA drug shortages, built with Python, dbt, and Streamlit.

## Features

- **ETL Pipeline**: Fetches and processes drug shortage data from FDA OpenFDA API
- **Data Transformation**: Uses dbt to create staging and mart tables with availability classifications
- **Dashboard**: Interactive Streamlit dashboard for visualizing shortage trends
- **Automated Updates**: Scheduled data refresh with availability status classification

## Quick Start

1. **Set up environment variables** in `.env`:
   ```
   SUPABASE_URL=your_supabase_url
   SUPABASE_ANON_KEY=your_key
   SUPABASE_SERVICE_KEY=your_service_key
   ```

2. **Run ETL pipeline**:
   ```bash
   python etl/fetch_fda_data.py
   ```
   A typicaly workflow:
   1. Create SQL model file (e.g., drug_shortage_episodes.sql)
   2. Add it to schema.yml with column descriptions and tests
   3. Run `dbt build --select drug_shortage_episodes` to create the table and validate it
   4. The table is now available in your Supabase database for your dashboard to query

3. **Transform data with dbt and check data lineage**:
   ```bash
   cd ds_db 
   dbt run
   dbt docs serve --host 0.0.0.0 --port 8080
   ```

4. **Launch dashboard**:
   ```bash
   python dashboard/dash_app.py
   ```

5. **Play around with it here**:
   ```
   https://drug-shortage-dashboard-ys.streamlit.app/
   ```

## Data Pipeline
Check out dbt lineage graph
   ```bash
   dbt docs serve --host 0.0.0.0 --port 8080
   ```


## Ideas from Marta
- capture market age (when is this active ingredient first approved)
- capture formulation 
- set up warning when the data schema changes (likely will trigger an error anyway)
- set up alert when there is an abnormal number of shortages (count unique number of APIs in a time frame)
