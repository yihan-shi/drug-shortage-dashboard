import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import logging
import hashlib

load_dotenv()

class OpenFDAETL:
    def __init__(self):
        # set up connection to PostgreSQL database
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_ANON_KEY")
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

        # OpenFDA API base URL
        self.base_url = "https://api.fda.gov/drug/shortages.json"
        
        # create logging for debugging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_date_range(self, days_back: int = 15) -> tuple[str, str]:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        return (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )

    def fetch_shortage_data(self, start_date: str, end_date: str, limit: int = 1000) -> Optional[List[Dict]]:
        search_query = f"update_date:[{start_date} TO {end_date}]"
        params = {
            "search": search_query,
            "limit": limit
        }
        
        try:
            self.logger.info(f"Fetching drug shortage data from {start_date} to {end_date}")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'results' in data:
                self.logger.info(f"Successfully fetched {len(data['results'])} records")
                return data['results']
            else:
                self.logger.warning("No results found in API response")
                return []
                
        # handle request exception
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from OpenFDA API: {e}")
            return None
        # handle JSON decode error
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON response: {e}")
            return None

    def get_existing_ids(self) -> Set[str]:
        """Fetch existing IDs from the staging table to avoid duplicates"""
        try:
            result = self.supabase.table('drug_shortages_staging').select('id').execute()
            existing_ids = {row['id'] for row in result.data} if result.data else set()
            self.logger.info(f"Found {len(existing_ids)} existing records in database")
            return existing_ids
        except Exception as e:
            self.logger.warning(f"Could not fetch existing IDs: {e}")
            return set()

    def generate_unique_id(self, record: Dict, existing_ids: Set[str]) -> str:
        """Generate a unique ID based on record content"""
        # Create a hash from key fields that make a record unique
        key_fields = [
            str(record.get('generic_name', '')),
            str(record.get('company_name', '')),
            str(record.get('presentation', '')),
            str(record.get('update_date', '')),
            str(record.get('package_ndc', ''))
        ]
        
        # Create base hash
        content = '|'.join(key_fields)
        base_hash = hashlib.md5(content.encode()).hexdigest()[:12]
        
        # Ensure uniqueness by adding counter if needed
        unique_id = base_hash
        counter = 1
        while unique_id in existing_ids:
            unique_id = f"{base_hash}_{counter:03d}"
            counter += 1
            
        existing_ids.add(unique_id)
        return unique_id

    def transform_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        transformed_records = []
        existing_ids = self.get_existing_ids()
        
        for record in raw_data:
            unique_id = self.generate_unique_id(record, existing_ids)
            transformed_record = {
                'id': unique_id,
                'generic_name': record.get('generic_name'),
                'company_name': record.get('company_name'),
                'presentation': record.get('presentation'),
                'update_type': record.get('update_type'),
                'update_date': record.get('update_date'),
                'availability': record.get('availability'),
                'related_info': record.get('related_info'),
                'resolved_note': record.get('resolved_note'), # might be null
                'reason_for_shortage': record.get('reason_for_shortage'), # might be null
                'therapeutic_category': record.get('therapeutic_category')[0],
                'status': record.get('status'),
                'change_date': record.get('change_date'),
                'date_discontinued': record.get('date_discontinued'),
                'availability_status': None, # to be filled later
                'ndc': record.get('package_ndc')
            }
            transformed_records.append(transformed_record)
        
        return pd.DataFrame(transformed_records)

    def load_to_staging(self, df: pd.DataFrame) -> bool:
        try:
            records = df.to_dict('records')
            
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            
            result = self.supabase.table('drug_shortages_staging').upsert(
                records, 
                on_conflict='id'
            ).execute()
            
            self.logger.info(f"Successfully loaded {len(records)} records to staging table")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to staging table: {e}")
            return False

    def run_weekly_etl(self):
        start_date, end_date = self.get_date_range(days_back=7)
        
        raw_data = self.fetch_shortage_data(start_date, end_date)
        
        if raw_data is None:
            self.logger.error("Failed to fetch data from API")
            return False
        
        if not raw_data:
            self.logger.info("No new data to process")
            return True
        
        df = self.transform_data(raw_data)
        
        if self.load_to_staging(df):
            self.logger.info("ETL process completed successfully")
            return True
        else:
            self.logger.error("ETL process failed during loading stage")
            return False

def main():
    etl = OpenFDAETL()
    success = etl.run_weekly_etl()
    
    if success:
        print("Weekly ETL completed successfully")
    else:
        print("Weekly ETL failed")
        exit(1)

if __name__ == "__main__":
    main()