import os, logging, argparse, traceback
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

from utils.settings_cash_points import input_output_dict, query_cash_points, clean_cash, clean_points
from utils.table_utils import PostgresInserter, extract_mongodb


# Create the log directory, based on script_directory, if it doesn't exist
script_directory = os.path.dirname(os.path.abspath(__file__))
log_directory = os.path.join(script_directory, 'log')
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Setup the log filename and configure the logger
log_filename = os.path.join(log_directory, os.path.splitext(os.path.basename(__file__))[0] + ".log")
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

def main(prefix, chunk_cap):  
    chunk_size = 500000 # 500k for fastest processing
    mongo_database = 'award_shopper'
    sort_column = '_id'
    sort_order = -1
    
    script_name = os.path.basename(os.path.abspath(__file__))
    run_name = os.path.splitext(script_name)[0]
    
    # Insert row into run table, generate run_id
    run_id = None # initialize run_id for start_run failures
    try:
        with PostgresInserter() as postgres_conn:
            run_details = f'chunk_cap = {chunk_cap}, chunk_size = {chunk_size}'
            run_id = postgres_conn.start_run(run_name, prefix, logger, details=run_details)
            logger.info(f'\nStarting run #{run_id}. prefix={prefix} chunk_size={chunk_size} chunk_cap={chunk_cap}')
    except Exception as e:
        logger.error(f'Error starting run. {e}')
        logger.error({traceback.format_exc()})
    
    # First processes cash, then points
    for table_type, table_type_dict in input_output_dict.items():
        output_table = f"{prefix}{table_type_dict['output_table']['table_name']}"
        columns_dict = table_type_dict['output_table']['table_columns']
        
        # Create Postgres output table
        try:
            with PostgresInserter() as postgres_conn:
                column_order = postgres_conn.create_table(output_table, columns_dict, logger)
        except Exception as e:
            logger.error(f"Error creating {output_table} with columns_dict:\n{columns_dict}\n{e}")
            logger.error({traceback.format_exc()})
        
        # Extract data from input tables, starting with "live" tables
        for input_table in table_type_dict['input_tables']:
            # Find the minimum _id for records with city = 'new-york'
            min_query = {'city': {'$in': ['new-york', 'New York']}}
            min_id = extract_mongodb(mongo_database, input_table, min_query, 1, sort_column, 1, logger)
            if not min_id.empty:
                min_id = min_id.iloc[0][sort_column]
                logger.info(f"min_id = {min_id} for {output_table}. min_query = {min_query}")
            
            # initialize chunking helper variables
            start_id = None
            chunk_n = 0
            rows_extracted = 0
            rows_inserted = 0
            hotel_group = input_table.split('_')[-1]
            try:
                while chunk_cap is None or chunk_n < chunk_cap:   # Loop until break or chunk cap exceeded
                    if start_id is not None and start_id < min_id: # Stop condition based on min_id
                        logger.info(f'Reached minimum _id {min_id}. Exiting loop.')
                        break
                    
                    # Extract data from mongoDB
                    extract_dt = datetime.utcnow() # datetime of data extraction, in UTC
                    query = query_cash_points(input_table, start_id)
                    df = extract_mongodb(mongo_database, input_table, query, chunk_size, sort_column, sort_order, logger)
                    
                    if df is None or df.empty:
                        logger.error(f'df empty or none')
                        break
                    elif sort_column not in df.columns: # Break loop if df is None or empty or missing sort_columns
                        logger.error(f'df missing sort_column = {sort_column} {df.columns} {df}')
                        break
                    
                    # Set start_id to the final row's _id. Next chunk will filter by _id < last_id
                    prior_id = start_id # For info debugging messages
                    start_id = df.iloc[-1][sort_column]
                    rows_extracted += len(df)
                    
                    # Clean data and add helper columns, depending on cash or points
                    helper_columns = {'run_id':run_id, 'hotel_group':hotel_group, 'chunk_n':chunk_n, 'extract_dt':extract_dt}
                    if table_type == 'cash':
                        clean_df = clean_cash(df, column_order, logger)
                    else:
                        clean_df = clean_points(df, column_order, logger)
                    
                    if clean_df is None or clean_df.empty: # Break loop if clean_df is None or empty
                        break
                    
                    # Insert into postgres with helper columns 
                    postgres_conn.insert_postgres(clean_df, output_table, logger, helper_columns, column_order)
                    
                    rows_inserted += len(clean_df)
                    chunk_n += 1 # increment chunk_n
                    logger.info(f'From {input_table} extracted {rows_extracted}, inserted {rows_inserted} from {prior_id} - {start_id}')
        
            except Exception as e:
                logger.error(f"Error piping data from {input_table} into {output_table}. {e} ")
                logger.error({traceback.format_exc()})
            
# python odynn_extract/extract_cash_points.py --prefix test_ --chunk_cap 2            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape and save REI data")
    parser.add_argument("--prefix", help="Optional prefix for table names. ", default="")
    parser.add_argument("--chunk_cap", help="Cap number of chunks for testing. None for unlimited", type=int, default=None)
    args = parser.parse_args()

    main(args.prefix, args.chunk_cap)