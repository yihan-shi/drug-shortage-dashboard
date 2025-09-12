import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Union
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
        
        # Load availability classification mapping
        self.availability_mapping = self.load_availability_mapping()

    def load_availability_mapping(self) -> Dict[str, str]:
        """Load the availability classification mapping from CSV"""
        try:
            mapping_file = 'data/shortage_2019_2024_unique_available_classification.csv'
            df = pd.read_csv(mapping_file)
            
            # Create mapping dictionary from availability text to status
            mapping = {}
            for _, row in df.iterrows():
                availability_text = row['availability']
                status = row['availability_status']
                if pd.notna(availability_text):
                    mapping[availability_text.strip()] = status
            
            self.logger.info(f"Loaded {len(mapping)} availability classifications from CSV")
            return mapping
            
        except Exception as e:
            self.logger.warning(f"Could not load availability mapping: {e}")
            return {}

    def classify_availability_status(self, availability_text: str, related_info: str = '', status: str = '') -> str:
        """
        Classify availability status using CSV mapping and checking availability, related_info, and status fields
        """
        # First try CSV mapping for availability text
        if availability_text and availability_text.strip():
            availability_clean = availability_text.strip()
            if availability_clean in self.availability_mapping:
                return self.availability_mapping[availability_clean]
        
        # Combine all text fields for comprehensive checking
        all_text = ' '.join([
            availability_text or '',
            related_info or '',
            status or ''
        ]).lower()
        
        # Discontinued patterns (highest priority)
        if any(pattern in all_text for pattern in ['discontinue', 'discontinued']):
            return 'discontinued'
        
        # Not available patterns
        if any(pattern in all_text for pattern in [
            'not available', 'unavailable', 'out of stock',
            'shortage', 'backorder', 'back order', 'supply disruption', 
            'manufacturing delay', 'resupply tbd', 'expected release',
            'next delivery', 'estimated availability'
        ]):
            return 'not available'
        
        # Limited availability patterns
        if any(pattern in all_text for pattern in [
            'limited', 'intermittent', 'restricted', 'allocated', 'allocation',
            'allocating', 'temporary shortage', 'reduced supply', 'under allocation'
        ]):
            return 'limited availability'
        
        # Available patterns
        if any(pattern in all_text for pattern in [
            'available', 'in stock', 'supply available', 'shipping', 'product available'
        ]):
            return 'available'
        
        return 'unclear'

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

    def get_existing_ids(self) -> Set[int]:
        """Fetch existing IDs from the staging table to avoid duplicates"""
        try:
            result = self.supabase.table('drug_shortages_staging').select('id').execute()
            existing_ids = {int(row['id']) for row in result.data if row['id'] is not None} if result.data else set()
            self.logger.info(f"Found {len(existing_ids)} existing records in database")
            return existing_ids
        except Exception as e:
            self.logger.warning(f"Could not fetch existing IDs: {e}")
            return set()

    def generate_unique_id(self, record: Dict, existing_ids: Set[int]) -> int:
        """Generate a unique integer ID based on record content"""
        # Create a hash from key fields that make a record unique
        key_fields = [
            str(record.get('generic_name', '')),
            str(record.get('company_name', '')),
            str(record.get('presentation', '')),
            str(record.get('update_date', '')),
            str(record.get('package_ndc', ''))
        ]
        
        # Create base hash and convert to integer
        content = '|'.join(key_fields)
        hash_obj = hashlib.md5(content.encode())
        base_id = int(hash_obj.hexdigest()[:8], 16)  # Use first 8 hex chars as int
        
        # Ensure uniqueness by incrementing if needed
        unique_id = base_id
        while unique_id in existing_ids:
            unique_id += 1
            
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
                'availability_status': self.classify_availability_status(
                    record.get('availability', ''),
                    record.get('related_info', ''),
                    record.get('status', '')
                ),
                'ndc': record.get('package_ndc'),
                'created_at': datetime.now().isoformat()
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

    def ensure_schema_exists(self):
        """Ensure the required views and indexes exist"""
        try:
            # Check if combined view exists, create if not
            check_view_sql = """
            SELECT COUNT(*) as view_count 
            FROM information_schema.views 
            WHERE table_name = 'drug_shortages_combined'
            """
            result = self.supabase.rpc('exec_sql', {'sql': check_view_sql}).execute()
            
            if result.data[0]['view_count'] == 0:
                self.logger.info("Creating drug_shortages_combined view...")
                with open('sql/create_staging_table.sql', 'r') as f:
                    sql_content = f.read()
                self.supabase.rpc('exec_sql', {'sql': sql_content}).execute()
                self.logger.info("Schema setup completed")
        except Exception as e:
            self.logger.warning(f"Could not auto-create schema: {e}")

    def promote_staging_to_historical(self):
        """Move staging data to historical table and clear staging"""
        try:
            # Get all staging data
            staging_result = self.supabase.table('drug_shortages_staging').select('*').execute()
            
            if not staging_result.data:
                self.logger.info("No staging data to promote")
                return True
            
            # Insert into historical table
            historical_result = self.supabase.table('drug_shortages_historical_classified').upsert(
                staging_result.data, 
                on_conflict='id'
            ).execute()
            
            promoted_count = len(staging_result.data)
            self.logger.info(f"Promoted {promoted_count} records to historical table")
            
            # Clear staging table
            self.supabase.table('drug_shortages_staging').delete().neq('id', 0).execute()
            self.logger.info("Staging table cleared successfully")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error promoting staging to historical: {e}")
            return False

    def run_weekly_etl(self):
        """Run weekly ETL: promote staging to historical, fetch new data, load to staging"""
        self.logger.info("Starting weekly ETL process")
        
        # Ensure schema exists
        self.ensure_schema_exists()

        # Always promote existing staging data to historical (this also clears staging)
        if not self.promote_staging_to_historical():
            self.logger.error("Failed to promote staging data to historical")
            return False
        
        # Fetch new data (last 15 days)
        start_date, end_date = self.get_date_range(days_back=15)
        raw_data = self.fetch_shortage_data(start_date, end_date)
        
        if raw_data is None:
            self.logger.error("Failed to fetch data from API")
            return False
        
        if not raw_data:
            self.logger.info("No new data to process")
            return True
        
        # Transform and load new data to staging
        df = self.transform_data(raw_data)
        
        if self.load_to_staging(df):
            self.logger.info("Weekly ETL process completed successfully")
            return True
        else:
            self.logger.error("Weekly ETL process failed during loading stage")
            return False

def main():
    etl = OpenFDAETL()
    
    # Count records before ETL
    staging_before = etl.supabase.table('drug_shortages_staging').select('id', count='exact').execute().count or 0
    historical_before = etl.supabase.table('drug_shortages_historical_classified').select('id', count='exact').execute().count or 0
    
    # Run ETL
    success = etl.run_weekly_etl()
    
    if success:
        # Count records after ETL
        staging_after = etl.supabase.table('drug_shortages_staging').select('id', count='exact').execute().count or 0
        historical_after = etl.supabase.table('drug_shortages_historical_classified').select('id', count='exact').execute().count or 0
        
        # Verify promotion worked correctly (upsert handles duplicates)
        if historical_after >= historical_before:
            historical_added = historical_after - historical_before
            print(f"✅ ETL completed: Historical {historical_before}→{historical_after} (+{historical_added}), Staging {staging_before}→{staging_after}")
            if historical_added < staging_before:
                print(f"   Note: {staging_before - historical_added} staging records were duplicates")
        else:
            print(f"❌ Historical count decreased: {historical_before}→{historical_after}")
            exit(1)
    else:
        print("❌ ETL failed")
        exit(1)

if __name__ == "__main__":
    main()