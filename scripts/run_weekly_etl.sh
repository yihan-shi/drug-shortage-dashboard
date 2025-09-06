#!/bin/bash

set -e

PROJECT_DIR="/Users/yihanshi/Desktop/Brookings/coding_projects/drug_shortage"
VENV_PATH="$PROJECT_DIR/.venv"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/weekly_etl_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$LOG_DIR"

{
    echo "Starting weekly drug shortage ETL at $(date)"
    echo "Project directory: $PROJECT_DIR"
    
    cd "$PROJECT_DIR"
    
    if [ ! -f .env ]; then
        echo "ERROR: .env file not found. Please create one based on .env.example"
        exit 1
    fi
    
    echo "Step 1: Activating virtual environment..."
    source "$VENV_PATH/bin/activate"
    
    echo "Step 2: Running OpenFDA data extraction..."
    python etl/fetch_fda_data.py
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Data extraction failed"
        exit 1
    fi
    
    echo "Step 3: Running dbt transformations..."
    cd ds_db
    
    export DBT_PROFILES_DIR="$PROJECT_DIR/ds_db"
    
    source ../.env
    
    echo "Running dbt debug to test connection..."
    dbt debug
    
    if [ $? -ne 0 ]; then
        echo "ERROR: dbt connection test failed"
        exit 1
    fi
    
    echo "Running dbt seed (if any seed files exist)..."
    dbt seed --select state:modified+ || true
    
    echo "Running dbt run..."
    dbt run
    
    if [ $? -ne 0 ]; then
        echo "ERROR: dbt run failed"
        exit 1
    fi
    
    echo "Running dbt test..."
    dbt test
    
    if [ $? -ne 0 ]; then
        echo "WARNING: Some dbt tests failed"
    fi
    
    echo "Weekly ETL completed successfully at $(date)"
    
} >> "$LOG_FILE" 2>&1

echo "ETL log saved to: $LOG_FILE"
cat "$LOG_FILE"