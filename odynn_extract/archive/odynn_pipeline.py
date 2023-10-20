import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from sqlalchemy import create_engine, text
import logging, math

import calendar, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

logging.basicConfig(filename='log_pipeline.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')  # Add date format to logging

import odynn_pipeline_cash

####### FUNCTIONS #######

#0 Generate create_table_command and create Postgresql table
def create_table(postgresql_uri, cash_flag, test_flag, output_columns):
    # Generate output_table_name
    cash_slug = 'cash' if cash_flag else 'points'
    test_slug = 'test' if test_flag else 'raw'    
    output_table_name = f'hotel_{cash_slug}_{test_slug}'
    
    # Generate a list of column names, in order
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

#1 Trim collection_names_all based on archived_flag and cash_flag
def trim_collection_names(collection_names_all, archived_flag, cash_flag):
    # If archived_flag is True, keep names containing 'archived_'. If False, keep names without 'archived_' (ie live)
    collection_names = [name for name in collection_names_all if ('archived_' in name) == archived_flag]
    
    # If cash_flag is True, keep names containing '_cash_'. If False, keep names missing '_cash_' (ie points)
    collection_names = [name for name in collection_names if ('_cash_' in name) == cash_flag]
    
    return collection_names

#2 Define the MongoDB query
def gen_query(last_id, hotel_keys_needed, hotel_filter, archived_flag, hotel_group):
    # Create base query dictionary to filter on city while handling bug in hyatt live tables
    query = {'city': 'New York'} if archived_flag is False and hotel_group == 'hyatt' else {'city': 'new-york'}
    
    # If last_id is defined, append an _id filter to the query dictionary
    if last_id:
        query['_id'] = {'$lt': ObjectId(last_id)}
    
    # If hotel_filter is defined, append a hotel_name_key filter to the query dictionary
    if hotel_filter is not None:
        query['hotel_name_key'] = {f'{hotel_filter}': hotel_keys_needed}
    
    return query

#3 Connect to MongoDB and Extract the Data
def extract_data_in_chunks(input_uri, collection_name, column_order, chunk_size, row_limit, starting_id, hotel_keys, hotel_filter, archived_flag):
    try:
        with MongoClient(input_uri) as client:
        # client = MongoClient(input_uri)
            collection = client.award_shopper[collection_name]
            hotel_group = collection_name.split('_')[-1]
            
            # Initialize 'last_id' and 'total_rows'
            last_id, total_rows = starting_id, 0
            
            # Limit to hotel_keys within the collection, for query efficiency
            hotel_keys_needed = [d['hotel_name_key'] for d in hotel_keys if d['hotel_group'] == hotel_group]

            while True and (row_limit is None or total_rows < row_limit): # Loop until break or row_limit exceeded
                query = gen_query(last_id, hotel_keys_needed, hotel_filter, archived_flag, hotel_group)
                cursor = collection.find(query).sort([("_id", -1)]).limit(chunk_size)
                
                df = pd.DataFrame()
                df = pd.DataFrame(list(cursor))
                
                if '_cash_' in collection_name:       # If extracting 'cash' data
                    if 'cash_value' in df.columns:  # Check if cash_value exists (early 'points' scrapes were saved to cash tables)
                        
                        # Drop unneeded columns for speed while adding any missing columns
                        df = df.reindex(columns=column_order)
                        df = df.drop(columns=['currency'])  # to avoid dupe columns after flattening cash_value dictionary

                        # Keep only rows where cash_value exists as a dictionary
                        df = df[df['cash_value'].apply(lambda x: isinstance(x, dict))]
                        
                        # If df is now empty, set to None
                        if df.empty:
                            df = None                
                    else:
                        break # Break loop if 'cash_value' is not a column
                
                else:       # If extracting points data
                    if 'points' in df.columns: 
                        # Drop unneeded columns for speed while adding expected columns (set to NULL) to any rows missing them
                        df = df.reindex(columns=column_order)

                        # Keep only rows where points exists and is not null
                        df = df[df['points'].notnull()]

                        # If df is now empty, set to None
                        if df.empty:
                            df = None
                    else:
                        break  # Break loop if 'points' is not a column
                
                if df is not None:
                    # Convert 'date' to datetime and handle errors
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')

                    # Drop rows where 'date' is NaT
                    df = df.dropna(subset=['date'])
                    
                    # Flatten and rename Cash columns
                    if '_cash_' in collection_name:
                        # Flatten 'cash_value' dictionary into distinct columns ('records' specifies the format)
                        df = pd.json_normalize(df.to_dict('records'))

                        # Rename columns
                        df = df.rename(columns={'cash_value.amount': 'cash_value', 'cash_value.currency': 'currency'})
                        
                        # Convert 'cash_value' to decimal
                        df['cash_value'] = df['cash_value'].apply(pd.to_numeric, errors='coerce')

                    # Standardize column order
                    df = df[column_order]
                    # convert _id field type to string so PostgreSQL can ingest
                    df['_id'] = df['_id'].astype(str)
                    
                    # Set last_id to the final row's _id. Next chunk will filter by _id < last_id
                    last_id = df.iloc[-1]['_id']
                    total_rows += len(df)
                    logging.info(f"extracted {total_rows} _id: {df['_id'].iloc[0]} - {df['_id'].iloc[-1]}, created_at: {df['created_at'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')} - {df['created_at'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    yield df
                
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        logging.info(f'Completed fetching data from {collection_name}')

#4 Insert data into Positco    
def insert_data(df, output_table_name, postgresql_uri):
    engine = create_engine(postgresql_uri)
    
    with engine.connect() as conn: # context manager ('with') closes connection if error
    # Insert the data into the table
        df.to_sql(output_table_name, engine, if_exists='append', index=False)

####### FUNCTIONS #######



####### INPUTS #########:

test_flag = True # True = output to hotel_cash_test (or hotel_points_test), False = hotel_cash_raw

archived_flag = False # True = archived tables, False = live tables
cash_flag = False # True = extract cash, False = extract points
hotel_filter = None # None = all hotels, '$nin' = exclude hotel_keys, '$in' = only include hotel_keys

# city = 'new-york'

starting_id = None # Set 'None' to extract from most recent row, or latest _id to start from prior _id
row_limit  = None # None to pipe unlimited rows for each collection
chunk_size = 1000000 # process 1m rows at a time for speed


# Define output_column names, types and order
if cash_flag:    
    output_columns = {    # Cash columns
        'hotel_group': 'TEXT',
        'hotel_name': 'TEXT',
        'date': 'DATE',
        'cash_value': 'NUMERIC',
        'currency': 'TEXT',
        'created_at': 'TIMESTAMP',
        'award_category': 'TEXT',
        'hotel_name_key': 'TEXT',
        'hotel_id': 'TEXT',
        '_id': 'TEXT'
    }
else:       
    output_columns = {    # Points columns
        'hotel_group': 'TEXT',
        'hotel_name': 'TEXT',
        'date': 'DATE',
        'points': 'NUMERIC',
        'created_at': 'TIMESTAMP',
        'award_category': 'TEXT',
        'points_level': 'TEXT',
        'hotel_name_key': 'TEXT',
        'hotel_id': 'TEXT',
        '_id': 'TEXT'
    }

collection_names_all = [
    'hotel_calendar_cash_hilton',
    'hotel_calendar_cash_hyatt',
    'hotel_calendar_cash_ihg',
    'hotel_calendar_cash_marriott',
    'archived_hotel_calendar_cash_hilton',
    'archived_hotel_calendar_cash_hyatt',
    'archived_hotel_calendar_cash_ihg',
    'archived_hotel_calendar_cash_marriott', 
    
    'hotel_calendar_hilton',
    'hotel_calendar_hyatt',
    'hotel_calendar_ihg',
    'hotel_calendar_marriott', 
    'archived_hotel_calendar_hilton',
    'archived_hotel_calendar_hyatt',
    'archived_hotel_calendar_ihg',
    'archived_hotel_calendar_marriott', 
]

input_uri = 'mongodb://data_reader:READ%23%23%24Awayzasdf33d!@44.212.136.220:27017/award_shopper?readPreference=primary&ssl=false&directConnection=true&authMechanism=DEFAULT&authSource=award_shopper'
hotel_keys = odynn_pipeline_cash.hotel_keys # Pulls list of original 48 new-york hotel keys for data sample    
postgresql_uri = 'postgresql://postgres:sa^5jv@ezf@localhost:5432/odynn' # outdated
postgresql_uri = 'postgresql://postgres:sa^5jv3o9ezf@localhost:5432/odynn'

####### INPUTS #######:


#### SCRIPT ####:
if __name__ == "__main__":
    start_time = datetime.now()
    print("Starting script:", start_time)
    logging.info(f'#### NEW RUN {start_time}####\n')
    logging.info(f'test_flag: {test_flag}  archived_flag: {archived_flag} cash_flag: {cash_flag} hotel_filter: {hotel_filter}')
    logging.info(f'starting_id: {starting_id}  row_limit: {row_limit} chunk_size: {chunk_size}')
    
    column_order, output_table_name = create_table(postgresql_uri, cash_flag, test_flag, output_columns) 
    
    collection_names = trim_collection_names(collection_names_all, archived_flag, cash_flag)

    # Pipe the data by collection and by chunk, into a df and then postgresql
    for collection_name in collection_names:
        logging.info('Starting data extraction for collection: %s', collection_name)
        try: 
            for df in extract_data_in_chunks(input_uri, collection_name, column_order, chunk_size, row_limit, starting_id, hotel_keys, hotel_filter, archived_flag):
                
                # Don't try to insert empty df (i.e, points columns instead of cash)
                if df is None:
                    continue
                
                # Insert data into Postico
                try:
                    insert_data(df, output_table_name, postgresql_uri)
                except Exception as e:
                    logging.error(f'Failed to insert_data to postgresql {output_table_name}. Error: {e}')
                    print(df)
                    continue # continue even if one chunk fails

        except Exception as e:
            logging.error(f'Failed to complete extract_data to df. Error: {e}')
            logging.error(f'Initial rows of failed df:\n {df.head().to_string()}')
            logging.error(f'Final rows of failed df:\n {df.tail().to_string()}')

    end_time = datetime.now()
    print("Finished script in {:.2f} seconds at {}".format((end_time - start_time).total_seconds(),end_time))

#### SCRIPT ####


# if cash_flag:    
#     output_columns = {    # Cash columns
#         'hotel_group': 'TEXT',
#         'hotel_name_key': 'TEXT',
#         'date': 'DATE',
#         'cash_value': 'NUMERIC',
#         'currency': 'TEXT',
#         'created_at': 'TIMESTAMP',
#         'award_category': 'TEXT',
#         'hotel_id': 'TEXT',
#         '_id': 'TEXT'
#     }
# else:       
#     output_columns = {    # Points columns
#         'hotel_group': 'TEXT',
#         'hotel_name_key': 'TEXT',
#         'date': 'DATE',
#         'points': 'NUMERIC',
#         'created_at': 'TIMESTAMP',
#         'award_category': 'TEXT',
#         'points_level': 'TEXT',
#         'hotel_id': 'TEXT',
#         '_id': 'TEXT'
#     }