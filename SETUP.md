# Drug Shortage ETL Pipeline Setup

This project creates an automated ETL pipeline to fetch weekly drug shortage updates from the OpenFDA API and process them through dbt into a clean data warehouse.

## Architecture Overview

```
OpenFDA API → Python ETL Script → Supabase (Staging) → dbt → Supabase (Marts)
```

## Setup Instructions

### 1. Environment Configuration

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```

2. Fill in your Supabase credentials in `.env`:
   ```
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_ANON_KEY=your_supabase_anon_key
   SUPABASE_SERVICE_KEY=your_supabase_service_key
   
   DBT_HOST=your_supabase_host
   DBT_PORT=5432
   DBT_USER=postgres
   DBT_PASSWORD=your_database_password
   DBT_DATABASE=postgres
   DBT_SCHEMA=public
   ```

### 2. Database Setup

1. Run the staging table creation script in your Supabase SQL editor:
   ```sql
   -- Copy and run the contents of sql/create_staging_table.sql
   ```

### 3. Install Dependencies

```bash
# If using uv (recommended)
uv sync

# Or with pip
pip install -r requirements.txt
```

### 4. Test the ETL Pipeline

1. **Test data extraction**:
   ```bash
   python etl/fetch_fda_data.py
   ```

2. **Test dbt transformations**:
   ```bash
   cd ds_db
   dbt debug  # Test connection
   dbt run     # Run transformations
   dbt test    # Run data quality tests
   ```

### 5. Set Up Automated Scheduling

#### Option A: Python Scheduler (Recommended)
Run the scheduler as a background service:
```bash
python scheduler.py
```

This will run the ETL every Monday at 6:00 AM.

#### Option B: System Cron Job
Add to your crontab:
```bash
crontab -e
# Add this line:
0 6 * * 1 /path/to/your/project/scripts/run_weekly_etl.sh
```

## Project Structure

```
drug_shortage/
├── etl/
│   └── fetch_fda_data.py          # OpenFDA API extraction script
├── ds_db/                         # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_drug_shortages.sql
│   │   │   └── sources.yml
│   │   └── marts/
│   │       ├── fact_drug_shortages.sql
│   │       ├── dim_manufacturers.sql
│   │       ├── shortage_summary.sql
│   │       └── schema.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/
│   └── run_weekly_etl.sh          # Orchestration script
├── sql/
│   └── create_staging_table.sql   # Database setup
├── logs/                          # ETL logs
├── scheduler.py                   # Python scheduler
├── .env.example                   # Environment template
└── SETUP.md                      # This file
```

## Data Models

### Staging Layer
- **stg_drug_shortages**: Clean view of raw OpenFDA data

### Marts Layer
- **fact_drug_shortages**: Main fact table with calculated fields
- **dim_manufacturers**: Manufacturer dimension with statistics
- **shortage_summary**: Weekly and monthly shortage summaries

## Monitoring

- ETL logs are saved in the `logs/` directory
- Each run creates a timestamped log file
- Check `logs/scheduler.log` for scheduling information

## Troubleshooting

1. **Connection Issues**: Run `dbt debug` to test database connection
2. **API Limits**: The OpenFDA API has rate limits; the script includes error handling
3. **Data Quality**: Check dbt test results for data quality issues
4. **Scheduling**: Ensure the scheduler script has proper permissions and environment access

## API Information

The ETL fetches data from: `https://api.fda.gov/drug/shortages.json`

Example query for date range:
```
https://api.fda.gov/drug/shortages.json?search=update_date:[2025-08-25%20TO%20DATE_END]&limit=1000
```

## Data Freshness

- ETL runs weekly (configurable)
- Fetches data from the last 7 days
- Uses upsert pattern to handle duplicates
- Historical data is preserved