import os
import argparse
import traceback
import pandas as pd
from datetime import datetime

from utils.settings_cash_points import input_output_dict, query_cash_points, clean_cash, clean_points
from utils.table_utils import PostgresInserter, extract_mongodb
from log.log_config import log_config

# Main function
def main(prefix, chunk_cap):
    env_str = "OUTPUT_MONGO_URI"  # Input table URI (Forbes sample)
    chunk_size = 1000000 #previously 500k
    mongo_database = 'awayzDB'
    sort_column = '_id'
    sort_order = -1
    dedupe_fields = [
        ['_id'],
        ['hotel_id', 'created_date', 'date']
    ]

    script_filename = os.path.basename(os.path.abspath(__file__))
    run_name = os.path.splitext(script_filename)[0]
    logger = log_config(script_filename)  # Configure the logger
    
    run_id = None
    try:
        with PostgresInserter() as postgres_conn:
            run_details = f'chunk_cap = {chunk_cap}, chunk_size = {chunk_size}'
            run_id = postgres_conn.start_run(run_name, prefix, logger, details=run_details)
            logger.info(f'\nStarting run #{run_id}. prefix={prefix} chunk_size={chunk_size} chunk_cap={chunk_cap}')
    except Exception as e:
        logger.error(f'Error starting run. {e}')
        logger.error({traceback.format_exc()})
        return

    for table_type, table_type_dict in input_output_dict.items():
        output_table = f"{prefix}{table_type_dict['output_table']['table_name']}"
        columns_dict = table_type_dict['output_table']['table_columns']
        
        try:
            with PostgresInserter() as postgres_conn:
                column_order = postgres_conn.create_table(output_table, columns_dict, logger)
        except Exception as e:
            logger.error(f"Error creating {output_table} with columns_dict:\n{columns_dict}\n{e}")
            logger.error({traceback.format_exc()})
            continue

        for input_table in table_type_dict['input_tables']:
            min_query = {}
            min_id = extract_mongodb(mongo_database, input_table, min_query, 1, sort_column, 1, None, logger, env_str)
            
            if min_id is None:
                continue
            
            if not min_id.empty:
                min_id = min_id.iloc[0][sort_column]
                logger.info(f"min_id = {min_id} for {input_table}. min_query = {min_query}")
            
            start_id = None
            chunk_n = 0
            rows_inserted = 0
            hotel_group = input_table.split('_')[-1]
            
            try:
                while chunk_cap is None or chunk_n < chunk_cap:
                    if start_id is not None and start_id <= min_id:
                        logger.info(f'Reached minimum _id {min_id}. Exiting loop.\n')
                        break
                    
                    extract_dt = datetime.utcnow()
                    query = query_cash_points(input_table, start_id)
                    df = extract_mongodb(mongo_database, input_table, query, chunk_size, sort_column, sort_order, dedupe_fields, logger, env_str)
                    
                    if df is None or df.empty:
                        logger.error(f'df empty or none')
                        break
                    elif sort_column not in df.columns:
                        logger.error(f'df missing sort_column = {sort_column} {df.columns} {df}')
                        break
                    
                    prior_id = start_id
                    start_id = df.iloc[-1][sort_column]
                    
                    helper_columns = {'run_id': run_id, 'hotel_group': hotel_group, 'input_table': input_table,'chunk_n': chunk_n, 'extract_dt': extract_dt}
                    if table_type == 'cash':
                        clean_df = clean_cash(df, column_order, logger)
                    else:
                        clean_df = clean_points(df, column_order, logger)
                    
                    if clean_df is None or clean_df.empty:
                        break
                    
                    postgres_conn.insert_postgres(clean_df, output_table, logger, helper_columns, column_order)
                    
                    rows_inserted += len(clean_df)
                    chunk_n += 1
                    logger.info(f'From {input_table} queried {chunk_size * chunk_n}, inserted {rows_inserted} from {prior_id} - {start_id}')
        
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