import os, time, logging, argparse
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from datetime import datetime

from log.log_config import log_config

hotel_templates_table_dict = {
    'hotel_templates': {
        'hotel_group':'VARCHAR(50)',
        'hotel_name': 'VARCHAR(70)',
        'hotel_id': 'VARCHAR(30)',
        'description': 'VARCHAR(100)',
        'address': 'TEXT',
        'city': 'VARCHAR(50)',
        'slug_city': 'VARCHAR(50)',
        'state': 'VARCHAR(30)',
        'state_code': 'VARCHAR(3)',
        'lat': 'NUMERIC',
        'long': 'NUMERIC',
        'review_count':'INTEGER',
        'review_rating':'NUMERIC',
        'country':'VARCHAR(40)',
        'country_code':'VARCHAR(5)',
        'telephone': 'VARCHAR(25)',
        'chain_rating':'NUMERIC',
        'created_at':'TIMESTAMP',
        '_id': 'TEXT',
    }
}

def main(prefix):
    # Determine script_name, and strip off extension to determine run_name
    script_name = os.path.basename(os.path.abspath(__file__))
    run_name = os.path.splitext(script_name)[0]
    
    # Configure logging and log start of run
    logger = log_config(f"{run_name}.log")
    logger.info(f"/n Starting {run_name}")
    
    
    create_table(hotel_templates_table_dict)
    run_id = None

def create_table(postgresql_uri, table_dict):
    # Generate list of column names via dictionary keys
    breakpoint()
    column_order = list(output_columns.keys())
    
    # Generate column_definitions as list of "NAME TYPE"
    column_definitions = [f"{column_name} {output_columns[column_name]}" for column_name in column_order]
    
    # Concatenate the definitions into the full CREATE TABLE command
    create_table_command = text(f"""
        CREATE TABLE IF NOT EXISTS {output_table_name} (
            {', '.join(column_definitions)}
        )
    """)
    
        # Open a Postgresql connection and create the table if it doesn't already exist
    engine = create_engine(postgresql_uri)
    with engine.connect() as conn:
        conn.execute(create_table_command)
        
    return column_order, output_table_name

    
    # Open a Postgresql connection and create the table if it doesn't already exist
    engine = create_engine(postgresql_uri)
    with engine.connect() as conn:
        conn.execute(create_table_command)
        
    return column_order, output_table_name

# Run script with optional prefix for testing
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Extract templates data to postgresql")
#     parser.add_argument("--prefix", help="Optional prefix for table names. ", default="")
#     #parser.add_argument("--page_cap", help="Cap number of pages for testing. None for unlimited", type=int, default=None)
#     args = parser.parse_args()
    
#     main(args.prefix)

if __name__ == "__main__":
    
    create_table(hotel_templates_table_dict)
    breakpoint()