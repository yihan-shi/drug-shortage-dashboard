#!/usr/bin/env python3
"""
Test script for the ETL pipeline - resets staging table before each run
"""

from etl.fetch_fda_data import OpenFDAETL

def main():
    print("Starting ETL test with staging table reset...")
    
    etl = OpenFDAETL()
    
    # Always reset staging table for testing
    success = etl.run_weekly_etl(reset_staging=True)
    
    if success:
        print("✅ Test ETL completed successfully")
        print("Staging table was reset and new data loaded")
    else:
        print("❌ Test ETL failed")
        exit(1)

if __name__ == "__main__":
    main()