import os, logging, traceback
from dotenv import load_dotenv
from pymongo import MongoClient
from itertools import islice
from datetime import datetime

# Helper function to slice an iterator into batches
def batch_iterator(iterator, batch_size):
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            return
        yield batch

# Function to extract and transfer data
def transfer_data(logger, src_col, dest_col, query_filter=None, batch_size = 10000):
    # Initialize counters
    success_count = 0
    error_count = 0
    
    # Create a cursor based on the filter
    cursor = src_col.find(query_filter) if query_filter else src_col.find()
    batch = [] # initialize batch list
        
    # Loop through each batch of documents
    for batch in batch_iterator(cursor, batch_size):
        try:
            dest_col.insert_many(batch)
            success_count += len(batch)
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            error_count += len(batch)

    logger.info(f"Successfully inserted {success_count} documents into {dest_col.name}")
    if error_count:
        logger.error(f"Failed to insert {error_count} documents into {dest_col.name}")

    return success_count, error_count
 
    
### SETTINGS ###
mongo_hotels = ['hilton', 'hyatt','ihg', 'marriott', 'accor']
mongo_tables = ['hotel_calendar', 'hotel_calendar_cash', 'hotel_directory_templates', 'archived_hotel_calendar', 'archived_hotel_calendar_cash']

start_datetime = datetime(2023, 10, 1, 0, 0, 0)
end_datetime = datetime(2023, 10, 2, 0, 0, 0)

date_filter = {
    'created_at': {'$gte': start_datetime, '$lt': end_datetime}
}

load_dotenv() # load URIs as environmental variables
src_uri = os.getenv("MONGO_URI")
dest_uri = os.getenv("OUTPUT_MONGO_URI")


### SCRIPT ###

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

# DB credentials
with MongoClient(src_uri) as src_client, MongoClient(dest_uri) as dest_client:
    src_db = src_client['award_shopper']
    dest_db = dest_client['award_shopper']

    for hotel in mongo_hotels:
            for table in mongo_tables:
                try:
                    table_name = table + '_' + hotel
                    logger.info(f'Transferring data for {table_name}')
                    
                    src_col = src_db[table_name]
                    dest_col = dest_db[table_name]
                    
                    query_filter = date_filter if 'hotel_calendar' in table_name else None
                        
                    transfer_data(logger, src_col, dest_col, query_filter = query_filter)
                    
                    logger.info("Data transfer complete for {table_name}!")
                        
                except Exception as e:
                    logger.error(f"Error with {table_name}: {e}")
                    logger.error(traceback.format_exc())