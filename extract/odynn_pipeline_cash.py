# date_booking is pretty confusing since it could refer to when the booking was made. Perhaps date_stay or date_room? Hm...

import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from sqlalchemy import create_engine, text
import logging, math, time, calendar

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


####### FUNCTIONS #######

#0 Connect to MongoDB and check table size
def get_collection_size(input_uri, collection_name):
    client = MongoClient(input_uri)
    db = client.award_shopper
    collection = db[collection_name]
    
    # Count the number of documents in the collection
    num_docs = collection.count_documents({})
    # Close the connection
    client.close()

    return num_docs

#1 Connect to MongoDB and Extract the Data
def extract_data_in_chunks(input_uri, collection_name, chunk_size, row_limit, hotel_keys, starting_id):
    client = MongoClient(input_uri)
    collection = client.award_shopper[collection_name]

    # Initialize 'last_id' and 'total_rows'
    last_id, total_rows = starting_id, 0
    columns_to_keep = ['_id', 'hotel_group', 'hotel_name_key', 'city', 'award_category', 'cash_value', 'date', 'created_at']

    # Select only the hotel_name_keys for this collection's hotel_group
    relevant_hotel_keys = [d['hotel_name_key'] for d in hotel_keys if d['hotel_group'] == collection_name.split('_')[-1]]

    while True and (row_limit is None or total_rows < row_limit):  # Loop until break or row_limit exceeded
        # MongoDB query variants. 'If last_id else' starts downward scan from a given _id, defined by starting_id
        # 1. Pull all new-york branches. 
        query = {'city': 'new-york', '_id': {'$lt': ObjectId(last_id)}} if last_id else {'city': 'new-york'}
        # 2. Pull new-york branches EXCLUDING relevant_hotel_keys (to INCLUDE, switch $nin to $in).
        # query = {'city': 'new-york', '_id': {'$lt': ObjectId(last_id)}, 'hotel_name_key': {'$nin': relevant_hotel_keys}} if last_id else {'city': 'new-york', 'hotel_name_key': {'$in': relevant_hotel_keys}}

        cursor = collection.find(query).sort([("_id", -1)]).limit(chunk_size)
        
        df = pd.DataFrame()
        df = pd.DataFrame(list(cursor))
        
        # breakpoint()
        if 'cash_value' in df.columns:
            # Drop unneeded columns for speed while adding expected columns (set to NULL) to any rows missing them
            df = df.reindex(columns=columns_to_keep)

            # Keep only rows where cash_value exists as a dictionary
            df = df[df['cash_value'].apply(lambda x: isinstance(x, dict))]

            # If df is now empty, set it to None and break the loop
            if df.empty:
                df = None
        else:
            break  # Break out of the loop if 'cash_value' is not in the DataFrame's columns

        # If 'df' is not None (i.e., if we have any documents to process)...
        if df is not None:
            # Convert 'date' to datetime and handle errors
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

            # Drop rows where 'date' is NaT
            df = df.dropna(subset=['date'])

            last_id = df.iloc[-1]['_id']
            total_rows += len(df)
            logging.info(f"extracted {total_rows} _id: {df['_id'].iloc[0]} - {df['_id'].iloc[-1]}, created_at: {df['created_at'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')} - {df['created_at'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')}")

            yield df

    client.close()
    logging.info(f'Completed fetching data from {collection_name}')


#2 Clean and Transform the Data
def clean_and_transform(df):
    # breakpoint()
    # skip cleaning if df is empty (i.e., points columns, rather than cash)
    if df is None:  
        return None

    # Flatten 'cash_value' field into separate columns
    df = pd.json_normalize(df.to_dict('records'))

    # Rename columns
    df = df.rename(columns={'date': 'date_booking', 'cash_value.amount': 'cash_value', 'cash_value.currency': 'currency'})
    
    # Convert 'cash_value' to decimal
    df['cash_value'] = df['cash_value'].apply(pd.to_numeric, errors='coerce')
    
    # convert _id field to a type PostgreSQL can ingest
    df['_id'] = df['_id'].astype(str)
    
    # Define the standard column order
    column_order = ['hotel_group', 'hotel_name_key', 'city', 'award_category', 'date_booking', 'cash_value', 'currency', 'created_at','_id']
    df = df[column_order]

    return df


#3 Connect to positco and create table
def create_table(postgresql_uri, table_name_out):
    # breakpoint()
    # PostgreSQL connection string
    engine = create_engine(postgresql_uri)

    # SQL command to create the table
    create_table_command = text(f"""
        CREATE TABLE IF NOT EXISTS {table_name_out} (
            hotel_group TEXT,
            hotel_name_key TEXT,
            city TEXT,
            date_booking DATE,
            cash_value NUMERIC,
            currency TEXT,
            created_at TIMESTAMP,
            award_category TEXT,
            _id TEXT
        )
    """)

    # Open a connection and execute the commands
    with engine.connect() as conn:
        # conn.execute(drop_table_command) # Doesn't seem to actually drop the data...
        conn.execute(create_table_command)


#4 Insert data into Positco    
def insert_data(df, table_name_out, postgresql_uri):
    # PostgreSQL connection string
    engine = create_engine(postgresql_uri)

    # Insert the data into the table
    df.to_sql(table_name_out, engine, if_exists='append', index=False)

####### FUNCTIONS #######


#### INPUTS ####:

input_uri = 'mongodb://data_reader:READ%23%23%24Awayzasdf33d!@44.212.136.220:27017/award_shopper?readPreference=primary&ssl=false&directConnection=true&authMechanism=DEFAULT&authSource=award_shopper'
collection_names = [
    'archived_hotel_calendar_cash_hilton',
    'archived_hotel_calendar_cash_hyatt',
    'archived_hotel_calendar_cash_ihg',
    'archived_hotel_calendar_cash_marriott', 
]
hotel_keys = [
    {'hotel_group': 'hilton', 'hotel_name_key': 'hampton-inn-manhattan-times-square-central'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hilton-garden-inn-new-york-west-35th-street'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hilton-club-the-quin-new-york'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hilton-garden-inn-new-york-times-square-north'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hilton-garden-inn-new-york-central-park-south-midtown-west'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'doubletree-by-hilton-hotel-new-york-times-square-west'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hilton-club-west-57th-street-new-york'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'distrikt-hotel-new-york-city-tapestry-collection-by-hilton'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hilton-garden-inn-new-york-midtown-park-ave'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hilton-garden-inn-new-york-times-square-south'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hampton-inn-manhattan-times-square-south'},
    {'hotel_group': 'hilton', 'hotel_name_key': 'hampton-inn-manhattan-35th-st-empire-state-bldg'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'the-beekman-a-thompson-hotel'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'andaz-5th-avenue'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'hotel-nyack'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'hotel-50-bowery'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'hyatt-herald-square-new-york'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'hyatt-union-square-new-york'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'hyatt-centric-times-square-new-york'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'gild-hall-a-thompson-hotel'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'hyatt-grand-central-new-york'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'hyatt-house-new-york-chelsea'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'park-hyatt-new-york'},
    {'hotel_group': 'hyatt', 'hotel_name_key': 'grayson-hotel'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'mr-mrs-smith-the-reform-club'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'hotel-indigo-nyc-financial-district'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'hotel-indigo-nyc-downtown-wall-street'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'holiday-inn-express-new-york-city-wall-street'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'holiday-inn-new-york-city-wall-street'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'holiday-inn-manhattan-financial-district'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'intercontinental-hotels-new-york-barclay'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'holiday-inn-express-manhattan-times-square-south'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'even-hotels-new-york-times-square-south'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'holiday-inn-express-manhattan-midtown-west'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'kimpton-hotel-theta'},
    {'hotel_group': 'ihg', 'hotel_name_key': 'hotel-indigo-lower-east-side-new-york'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'the-times-square-edition'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'renaissance-new-york-chelsea-hotel'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'four-points-by-sheraton-midtown-times-square'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'courtyard-new-york-manhattan-midtown-east'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'moxy-nyc-lower-east-side'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'sheraton-new-york-times-square-hotel'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'renaissance-new-york-times-square-hotel'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'residence-inn-new-york-manhattan-central-park'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'towneplace-suites-new-york-manhattan-chelsea'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'four-points-by-sheraton-new-york-downtown'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'courtyard-new-york-manhattan-upper-east-side'},
    {'hotel_group': 'marriott', 'hotel_name_key': 'delta-hotels-new-york-times-square'}
]

table_name_out = 'hotel_cash_test' # hotel_cash_raw for production
starting_id = None # Set to None for complete run, otherwise scans down from this _id (for ongoing extraction, will need to modify script to scan upward from starting id). Also, not copied over to awards script!
row_limit = None # None to pipe unlimited rows for each collection
chunk_size = 400000 # process 400k rows at a time, faster than 25k
postgresql_uri = 'postgresql://postgres:heytimmy@localhost:5432/odynn'

#### INPUTS ####:


#### SCRIPT ####:
if __name__ == "__main__":
    logging.basicConfig(filename='pipeline_cash.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')  # Add date format to logging
    start_time = datetime.now()
    print("Starting script:", start_time)
    logging.info(f'\n#### NEW RUN starting {start_time}####\n')

    create_table(postgresql_uri, table_name_out)  # create the table

    # Pipe the data by collection and by chunk, into a df and then postgresql
    for collection_name in collection_names:
        logging.info('Starting data extraction for collection: %s', collection_name)
        try:
            
            for df in extract_data_in_chunks(input_uri, collection_name, chunk_size, row_limit, hotel_keys, starting_id):
                # Clean and transform the data
                df = clean_and_transform(df)
                
                # Don't try to insert empty df (i.e, points columns instead of cash)
                if df is None:
                    continue
                
                # Insert data into Postico
                try:
                    insert_data(df, table_name_out, postgresql_uri)
                except Exception as e:
                    logging.error('Failed to complete insert_data to postgresql. Error: %s', e)
                    continue # continue even if one chunk fails

        except Exception as e:
            logging.error('Failed to complete extract_data to df. Error: %s', e)
        logging.info(f'{collection_name} data extraction complete')

    end_time = datetime.now()
    print("Finished script in {:.2f} seconds at {}".format((end_time - start_time).total_seconds(),end_time))

    # breakpoint()
    
    # Removed from clean_and_transform function, after checking if df = None
    # Redundant? Or may catch error with last Marriott chunk... drops rows where cash_value is not a dictionary
    # if 'cash_value' in df.columns:
    #     df = df[df['cash_value'].apply(lambda x: isinstance(x, dict))]
    # else: # removes the df if NO rows contain cash_Value
    #     df = df.iloc[0:0]  # This will create an empty DataFrame with the same columns