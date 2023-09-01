import os, logging, argparse
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

from utils.settings_cash_points import input_output_settings, clean_cash, clean_points
from utils.table_utils import PostgresInserter, extract_mongodb

log_filename = 'log/'+ os.path.splitext(os.path.basename(__file__))[0] + ".log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def main(prefix, chunk_cap):  
    chunk_size = 50000 # 500k for fastest processing
    
    script_name = os.path.basename(os.path.abspath(__file__))
    run_name = os.path.splitext(script_name)[0]
    
    # Insert row into run table, generate run_id
    try:
        with PostgresInserter as postgres_conn:
            postgres_conn.start_run(run_name, prefix, logger)
    except Exception as e:
        logger.error(f'Error starting run')
    
    # First processes cash, then points
    for table_type in input_output_settings:
        output_table = f"{prefix}{table_type['output_table']['table_name']}"
        columns_dict = table_type['output_table']['table_columns']
        
        # Create Postgres output table
        try:
            with PostgresInserter as postgres_conn:
                columns_list = postgres_conn.create_table(output_table, columns_dict, logger)
        except Exception as e:
            logger.error(f"Error creating {output_table} with columns_dict:\n{columns_dict}\n{e}")
        
        # Extract data from input tables, starting with "live" tables
        for input_table in table_type.input_tables:
            
            # initialize chunking helper variables
            start_id = None
            n = 0
            try:
                with PostgresInserter as postgres_conn:        
                    while chunk_cap is None or n < chunk_cap:   # Loop until break or chunk cap exceeded
                        df = extract_mongodb(input_table, start_id)
                        
                        if df is None: # Break loop if df is empty
                            break
                        
                        # Clean data, depending on cash or points
                        if table_type == 'cash':
                            clean_df = clean_cash(df)
                        else:
                            clean_df = clean_points(df)
                        
                        if clean_df is None: # Break loop if clean_df is empty
                            break
                        
                        postgres_conn.insert_postgresql(clean_df)
            except Exception as e:
                logger.error(f"Error piping data from {input_table} into {output_table} for ")
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape and save REI data")
    parser.add_argument("--prefix", help="Optional prefix for table names. ", default="")
    parser.add_argument("--chunk_cap", help="Cap number of chunks for testing. None for unlimited", type=int, default=None)
    args = parser.parse_args()

    main(args.prefix, args.chunk_cap)
    
    
