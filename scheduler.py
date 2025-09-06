import schedule
import time
import subprocess
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_weekly_etl():
    logger.info("Starting scheduled weekly ETL job")
    
    try:
        project_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(project_dir, 'scripts', 'run_weekly_etl.sh')
        
        result = subprocess.run(
            ['/bin/bash', script_path],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        if result.returncode == 0:
            logger.info("Weekly ETL completed successfully")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"Weekly ETL failed with return code {result.returncode}")
            logger.error(f"Error output: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("Weekly ETL timed out after 1 hour")
    except Exception as e:
        logger.error(f"Error running weekly ETL: {e}")

def main():
    logger.info("Drug Shortage ETL Scheduler started")
    
    schedule.every().monday.at("06:00").do(run_weekly_etl)
    
    logger.info("Scheduled weekly ETL for every Monday at 6:00 AM")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()