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

3. **Transform data with dbt**:
   ```bash
   cd ds_db && uv run --env-file ../.env dbt run
   ```

4. **Launch dashboard**:
   ```bash
   streamlit run dashboard/streamlit_app.py
   ```

5. **Play around with it here**:
   ```
   https://drug-shortage-dashboard-ys.streamlit.app/
   ```

## Data Pipeline

- `historical_data_clean.ipynb` - Historical data cleaning and classification
- `etl/fetch_fda_data.py` - Fetches FDA data and classifies availability status
- `ds_db/models/staging/` - dbt staging models for data cleaning
- `ds_db/models/marts/` - dbt mart models for analytics (episodes, summaries)
- `dashboard/streamlit_app.py` - Interactive visualization dashboard

## Updating Availability Classifications

If you need to redefine availability_status:
1. Update classifications in `historical_data_clean.ipynb` to update historical data and reupload

2. Update `load_availability_mapping()` and `classify_availability_status()` in `fetch_fda_data.py`
