import os, time, logging, argparse, traceback
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

from log.log_config import log_config

hotel_templates_table_tuple = ('hotel_templates', {
        'hotel_group':'TEXT',
        'hotel_name': 'TEXT', # name (reserved word in postgresql)
        'hotel_id': 'TEXT',
        'description': 'TEXT',
        'address': 'TEXT',
        'city': 'TEXT',
        'slug_city': 'TEXT',
        'state': 'TEXT',
        'state_code': 'TEXT',
        'latitude': 'NUMERIC', # coordinates.latitude
        'longitude': 'NUMERIC', # coordinates.longitude
        'currency': 'TEXT', # cash_value.currency
        'review_count':'INTEGER',
        'review_rating':'NUMERIC',
        'country':'TEXT',
        'country_code':'TEXT',
        'telephone': 'TEXT',
        'chain_rating':'NUMERIC',
        'created_at':'TIMESTAMP',
        '_id': 'TEXT PRIMARY KEY',
        'run_id':'INTEGER',
        'dt':'TIMESTAMP',
    }
)

run_table_tuple = ('run', {
        'run_id': 'SERIAL PRIMARY KEY',
        'run_name': 'TEXT NOT NULL',
        'details': 'TEXT',
        'run_dt': 'TIMESTAMP NOT NULL',
    }
)

# rename_dict = None if no renaming needed
rename_dict = { 
    'name': 'hotel_name',
}

collection_names = (
    'hotel_directory_templates_hilton',
    'hotel_directory_templates_hyatt',
    'hotel_directory_templates_ihg',
    'hotel_directory_templates_marriott',
)

def main(prefix):
    # Determine script_name, and strip off extension to determine run_name
    script_name = os.path.basename(os.path.abspath(__file__))
    run_name = os.path.splitext(script_name)[0]
    
    # Configure logging and log start of run
    logger = log_config(f"{run_name}.log")
    logger.info(f"/n Starting {run_name}")
    
    # Create output tables in postgresql if needed
    create_table(run_table_tuple, prefix, logger)
    columns_list, output_table_name = create_table(hotel_templates_table_tuple, prefix, logger)
    
    # Generate run_id by inserting row into 'run' table
    run_id = start_run(run_name, prefix, logger)
    
    for collection_name in collection_names:
        df, dt = extract_data(collection_name, columns_list, logger, rename_dict)
        helper_columns = {'run_id': run_id, 'dt': dt}
        insert_to_sql(df, output_table_name, logger, helper_columns)

def insert_to_sql(df, output_table_name, logger, helper_columns=None):
    load_dotenv() # load URI (can put this outside function too)
    postgresql_uri = os.environ.get("POSTGRESQL_URI")
    
    df = add_helper_columns(df, helper_columns)
    
    if df is None or df.empty:
        logger.warning(f"No data to save to {output_table_name}. Skipping.")
        return
    
    try:
        engine = create_engine(postgresql_uri)
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT") # automatically commit insertions
            df.to_sql(output_table_name, engine, index=False, if_exists='append')
            logger.info(f"Saved {len(df)} rows to {output_table_name}")
            
    except Exception as e:
        logger.error(f"Error saving data to {output_table_name}: \n{traceback.format_exc()}")
        
def add_helper_columns(df: pd.DataFrame, helper_columns: dict) -> pd.DataFrame:
    if helper_columns is not None:
        if df is None:
            return pd.DataFrame([helper_columns])
        else:
            for column, value in helper_columns.items():
                df[column] = value
    return df

def start_run(run_name, prefix, logger, details=''):
    run_dt = datetime.utcnow()
    output_table_name = f'{prefix}run'
    insert_query = text(f"INSERT INTO {output_table_name} (run_dt, run_name, details) VALUES (:run_dt, :run_name, :details) RETURNING run_id;")
    
    load_dotenv() # load URI (can put this outside function too)
    postgresql_uri = os.environ.get("POSTGRESQL_URI")   
    run_id = None # initialized in case of error
    try:
        engine = create_engine(postgresql_uri)
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT") # automatically commit insertions
            result = conn.execute(insert_query, {'run_dt': run_dt, 'run_name': run_name, 'details': details})
            run_id = result.fetchone()[0]
            return run_id
    except Exception as e:
        logger.error(f'Error starting run {run_id} of {run_name} into {output_table_name}: {e} \n {insert_query}')
        
        
# query={} extracts all rows. Modify to filter, sort and/or start from a specific _id
def extract_data(collection_name, columns_list, logger, rename_dict=None, query={}):
    load_dotenv() # load URI (can put this outside function too)

    try:
        mongo_uri = os.environ.get("MONGO_URI")
        with MongoClient(mongo_uri) as client:
            collection = client.award_shopper[collection_name]
            cursor = collection.find(query)#.sort([("_id", -1)]).limit(chunk_size)
            
            df = pd.DataFrame() # initialize df
            df = pd.DataFrame(list(cursor))
            
            # Rename fields if needed
            if rename_dict is not None:
                df.rename(columns=rename_dict, inplace=True)

            # Unnest nested fields like 'coordinates'
            if 'coordinates' in df.columns:
                df['latitude'] = df['coordinates'].apply(lambda x: x.get('latitude', None))
                df['longitude'] = df['coordinates'].apply(lambda x: x.get('longitude', None))
                # df.drop('coordinates', axis=1, inplace=True)
            if 'cash_value' in df.columns:
                df['currency'] = df['cash_value'].apply(lambda x: x.get('currency', None))
                # df.drop('cash_value', axis=1, inplace=True)
            
            # Create a list containing only columns in the df (drops 2 helper columns)
            filtered_columns_list = [col for col in columns_list if col in df.columns]
            df = df[filtered_columns_list] # Reorder and remove unneeded columns
            
            # convert _id field type to string so PostgreSQL can ingest
            df['_id'] = df['_id'].astype(str)
            df = df.applymap(lambda x: None if x == '' else x) # Replace empty strings with None, otherwise Numeric types fail upon insertion

            dt = datetime.utcnow() # determine dt of extraction
    
            return df, dt
    except Exception as e:
        logger.error(f'Error with extract_data on {collection_name}')
    
def create_table(table_tuple, prefix, logger):
    output_table_name, columns = table_tuple
    output_table_name = f'{prefix}{output_table_name}' if prefix is not None else output_table_name # Add prefix, if exists
    load_dotenv() # load URI (can put this outside function too)
    postgresql_uri = os.environ.get("POSTGRESQL_URI")
    
    # Generate columns_Sql as list of "NAME TYPE" for create table query
    columns_sql = [f"{column_name} {columns[column_name]}" for column_name in columns]
    columns_list = list(columns.keys()) # Extract a list of column names
    
    # Concatenate the definitions into the full CREATE TABLE command
    create_table_query = text(f"""
        CREATE TABLE IF NOT EXISTS {output_table_name} (
            {', '.join(columns_sql)}
        )
    """)
    
    # Open a Postgresql connection and create the table if it doesn't already exist
    engine = create_engine(postgresql_uri)
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        logger.info(f"Executing query: {create_table_query}")
        conn.execute(create_table_query)
        
    return columns_list, output_table_name

# Run script with optional prefix for testing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract templates data to postgresql")
    parser.add_argument("--prefix", help="Optional prefix for table names. ", default="")
    args = parser.parse_args()
    
    main(args.prefix)