import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import logging, math

import calendar, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

logging.basicConfig(filename='data_pipeline.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')  # Add date format to logging

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
def extract_data_in_chunks(input_uri, collection_name, chunk_size, row_limit, hotel_keys):
    
    client = MongoClient(input_uri)
    collection = client.award_shopper[collection_name]
    
     # Initialize 'last_id' and 'total_rows'
    last_id, total_rows = None, 0
    columns_to_keep = ['_id', 'hotel_group','hotel_name_key','city','award_category','cash_value','date','created_at']
    
    # Select only the hotel_name_keys for this collection's hotel_group
    relevant_hotel_keys = [d['hotel_name_key'] for d in hotel_keys if d['hotel_group'] == collection_name.split('_')[-1]]
    
    while total_rows < row_limit:
        # Build the query to fetch rows using indexed _id, hotel_name_key, and new-york region
        query = {'city': 'new-york', '_id': {'$gt': last_id}, 'hotel_name_key': {'$in': relevant_hotel_keys}} if last_id else {'city': 'new-york', 'hotel_name_key': {'$in': relevant_hotel_keys}}
        cursor = collection.find(query).sort([("_id", -1)]).limit(chunk_size)
        df = pd.DataFrame(list(cursor))
        # breakpoint()
        
        # Keep only the necessary columns, reindex fills Nan for missing columns to avoid breaking
        df = df.reindex(columns=columns_to_keep)
        
        # Filter for Tuesday observations, early to speed up processing
        # df = df[df['created_at'].dt.dayofweek == 1]  

        # If the 'cash_value' column exists in the DataFrame...
        if 'cash_value' in df.columns:
            # Keep only rows where cash_value exists
            df = df[df['cash_value'].apply(lambda x: isinstance(x, dict))]
            # If df is now empty, set it to None
            if df.empty:
                df = None
        else:
            # if cash_value does not exist anywhere in the df, set it to None
            df = None

        logging.info(f'Fetched {total_rows} rows so far from {collection_name}')
        
        # If 'df' is not None (i.e., if we have any documents to process)...
        if df is not None:
            # Convert 'date' to datetime and handle errors
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

            # Drop rows where 'date' is NaT
            df = df.dropna(subset=['date'])

            last_id = df.iloc[-1]['_id']
            total_rows += len(df)
            df = df.drop(columns=['_id'])
            yield df
        
    client.close()
    logging.info(f'Completed fetching data from {collection_name}')

#2 Clean and Transform the Data
def clean_and_transform(df):
    # breakpoint()
    # skip cleaning if df is empty (i.e., points columns, rather than cash)
    if df is None:  
        return None
    
    # drop rows where cash_value is not a dictionary
    if 'cash_value' in df.columns:
        df = df[df['cash_value'].apply(lambda x: isinstance(x, dict))]
    else: # removes the df if NO rows contain cash_Value
        df = df.iloc[0:0]  # This will create an empty DataFrame with the same columns

    # Flatten 'cash_value' field into separate columns
    df = pd.json_normalize(df.to_dict('records'))

    # Rename columns
    df = df.rename(columns={'date': 'date_booking', 'cash_value.amount': 'cash_value', 'cash_value.currency': 'currency'})

    # Convert 'date_booking' to date type
    df['date_booking'] = pd.to_datetime(df['date_booking'])
    
    # Convert 'cash_value' to decimal
    df['cash_value'] = df['cash_value'].apply(pd.to_numeric, errors='coerce')
    
    # Define the standard column order
    column_order = ['hotel_group', 'hotel_name_key', 'city', 'award_category', 'date_booking', 'cash_value', 'currency', 'created_at']
    df = df[column_order]

    return df


#3 Connect to positco and create table
def create_table(postgresql_uri, table_name_out):
    # breakpoint()
    # PostgreSQL connection string
    engine = create_engine(postgresql_uri)

    # SQL command to drop the table if it exists
    drop_table_command = text(f"DROP TABLE IF EXISTS {table_name_out}")

    # SQL command to create the table
    create_table_command = text("""
        CREATE TABLE IF NOT EXISTS hotel_data (
            hotel_group TEXT,
            hotel_name_key TEXT,
            city TEXT,
            award_category TEXT,
            date_booking DATE,
            cash_value NUMERIC,
            currency TEXT,
            created_at TIMESTAMP
        )
    """)

    # Open a connection and execute the commands
    with engine.connect() as conn:
        conn.execute(drop_table_command)
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

table_name_out = 'hotel_cash'
row_limit = None # None to pipe unlimited rows for each collection
chunk_size = 100000 # process 100k rows at a time, faster than 25k. But 6/7 days dropped so more like ~14k per chunk
postgresql_uri = 'postgresql://postgres:heytimmy@localhost:5432/odynn'

#### INPUTS ####:


#### SCRIPT ####:
if __name__ == "__main__":
    start_time = time.time()
    now = datetime.now()
    print("Current time:", now)
    logging.info(f'#### NEW RUN starting at {now}####')

    create_table(postgresql_uri, table_name_out)  # create the table

    # Pipe the data by collection and by chunk, into a df and then postgresql
    for collection_name in collection_names:
        logging.info('Starting data extraction for collection: %s', collection_name)
        try:
            # collection_size = get_collection_size(input_uri, collection_name)
            # logging.info(f'Collection has {collection_size} rows')
            
            for df in extract_data_in_chunks(input_uri, collection_name, chunk_size, row_limit, hotel_keys):
                # breakpoint()
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
            # breakpoint()
            logging.error('Failed to complete extract_data to df. Error: %s', e)
        logging.info('Data extraction completed')

    end_time = time.time()
    runtime = end_time - start_time
    print("Runtime: {:.2f} seconds".format(runtime))

    breakpoint()