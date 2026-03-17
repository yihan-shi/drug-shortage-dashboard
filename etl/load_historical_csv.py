# this is not meant to be run once, to update historical data from 2014-2025

import pandas as pd
import hashlib
import os
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CSV_PATH = 'data/drug_shortage_historical/shortage_2014_2025_full.csv'


def generate_id(generic_name: str, company_name: str, presentation: str, update_date: str, ndc: str) -> int:
    key_fields = [
        str(generic_name or ''),
        str(company_name or ''),
        str(presentation or ''),
        str(update_date or ''),
        str(ndc or '')
    ]
    content = '|'.join(key_fields)
    hash_obj = hashlib.md5(content.encode())
    return int(hash_obj.hexdigest()[:8], 16)


def classify_shortage_status(update_type: str, status: str) -> str:
    if pd.isna(update_type) or pd.isna(status):
        return None
    update_type = str(update_type).strip().lower()
    status = str(status).strip().lower()

    if update_type == 'new' and status == 'current':
        return 'new'
    elif (update_type in ['revised', 'reverified'] and status == 'current') or (status == 'currently in shortage'):
        return 'continued'
    elif status == 'resolved':
        return 'ended'
    elif status in ['to be discontinued', 'discontinuation']
        return 'discontinued'
    return None


def load_csv_to_historical(csv_path: str):
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_ANON_KEY")
    supabase: Client = create_client(supabase_url, supabase_key)

    logger.info(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows")

    # Parse update_date, fall back to year+month from columns if missing
    df['update_date'] = pd.to_datetime(df['update_date'], errors='coerce')
    fallback_dates = pd.to_datetime(
        df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2) + '-01',
        errors='coerce'
    )
    df['update_date'] = df['update_date'].fillna(fallback_dates)
    df['update_date'] = df['update_date'].dt.strftime('%Y-%m-%d').where(df['update_date'].notna(), None)

    # Fetch existing IDs to detect collisions
    result = supabase.table('drug_shortages_classified_raw').select('id').execute()
    existing_ids = {int(row['id']) for row in result.data if row['id'] is not None}
    logger.info(f"Found {len(existing_ids)} existing records in historical table")

    records = []
    seen_ids = set(existing_ids)

    for _, row in df.iterrows():
        update_date = row.get('update_date')

        base_id = generate_id(
            row.get('generic_name'),
            row.get('company_name'),
            row.get('presentation'),
            update_date,
            row.get('ndc')
        )

        # Resolve collision the same way the ETL does
        unique_id = base_id
        while unique_id in seen_ids:
            unique_id += 1
        seen_ids.add(unique_id)

        records.append({
            'id': unique_id,
            'generic_name': row.get('generic_name') if not pd.isna(row.get('generic_name')) else None,
            'company_name': row.get('company_name') if not pd.isna(row.get('company_name')) else None,
            'presentation': row.get('presentation') if not pd.isna(row.get('presentation')) else None,
            'update_type': row.get('update_type') if not pd.isna(row.get('update_type')) else None,
            'update_date': update_date,
            'availability': None,
            'related_info': None,
            'resolved_note': None,
            'reason_for_shortage': None,
            'therapeutic_category': row.get('therapeutic_category') if not pd.isna(row.get('therapeutic_category')) else None,
            'status': row.get('status') if not pd.isna(row.get('status')) else None,
            'status_change_date': None,
            'change_date': None,
            'date_discontinued': None,
            'shortage_status': classify_shortage_status(row.get('update_type'), row.get('status')),
            'ndc': row.get('ndc') if not pd.isna(row.get('ndc')) else None,
            'created_at': datetime.now().isoformat()
        })

    # Insert in batches of 500
    batch_size = 500
    inserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        supabase.table('drug_shortages_classified_raw').upsert(
            batch,
            on_conflict='id',
            ignore_duplicates=True
        ).execute()
        inserted += len(batch)
        logger.info(f"Inserted batch {i // batch_size + 1}: {inserted}/{len(records)} rows")

    logger.info(f"Done. {len(records)} rows processed.")


if __name__ == '__main__':
    load_csv_to_historical(CSV_PATH)
