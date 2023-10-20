from datetime import datetime
from itertools import islice
from pymongo import MongoClient
import os
from dotenv import load_dotenv

from log.log_config import log_config

def get_id_range(logger, src_col, start_datetime, end_datetime):
    logger.info(f"Getting id_range")
    
    # Find the max_id within the date range by scanning downward from end_datetime
    max_doc = src_col.find({
        'created_at': {'$lt': end_datetime},
    }).sort([('_id', -1)]).limit(1)
    
    max_doc = list(max_doc)  # Convert cursor to list to access the result

    if len(max_doc) > 0:
        max_id = max_doc[0]['_id']
        logger.info(f"Successfully got max_doc {max_doc}")
        
        # Find the min_id by scanning downward from the max_id - much faster than sort asending and scanning upward
        min_doc = src_col.find_one({
            'created_at': {'$gte': start_datetime, '$lt': end_datetime},
            '_id': {'$lt': max_id}
        })
        
        min_id = min_doc['_id'] if min_doc else None
    else:
        min_id = None
        max_id = None
    
    max_id = max_doc[0]['_id'] if len(max_doc) > 0 else None
    
    logger.info(f"Successfully got min_doc {min_doc}")
    return min_id, max_id

def batch_iterator(iterator, batch_size):
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            return
        yield batch

def transfer_data(logger, src_col, dest_col, query_filter=None, batch_size=100000):
    # Clear the destination collection, if exists
    dest_col.delete_many({})
    
    if query_filter:
        # Because only _id is indexed, much faster to find max/min _id for a given date range versus filtering on created_at directly
        min_id, max_id = get_id_range(logger, src_col, query_filter.get('created_at').get('$gte'), query_filter.get('created_at').get('$lt'))
        
        # Replace query_filter to only include _id range
        if min_id and max_id:
            query_filter = {'_id': {'$gte': min_id, '$lte': max_id}}

    logger.info(f"Attempting to run query = {query_filter} on {src_col}")
    cursor = src_col.find(query_filter)
    logger.info(f"Successfully created cursor with query = {query_filter}")
    
    success_count = 0
    error_count = 0
    
    for batch in batch_iterator(cursor, batch_size):
        try:
            dest_col.insert_many(batch)
            logger.debug(f"Successfully inserted {len(batch)} documents into {src_col.name}.")
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            error_count += len(batch)
    
    logger.info(f"Successfully inserted {success_count} documents into {dest_col.name}")
    if error_count:
        logger.error(f"Failed to insert {error_count} documents into {dest_col.name}")

# Load environment variables
load_dotenv()
src_uri = os.getenv("MONGO_URI")
dest_uri = os.getenv("OUTPUT_MONGO_URI")

mongo_hotels = [
    #'hilton', 'hyatt','ihg', 'marriott', 
    # 'accor',
    'choice']
mongo_tables = [
    'hotel_calendar', 'hotel_calendar_cash', 'hotel_directory_templates',
    'archived_hotel_calendar', 'archived_hotel_calendar_cash'
]

start_datetime = datetime(2023, 10, 1, 0, 0, 0)
end_datetime = datetime(2023, 10, 2, 0, 0, 0)

date_filter = {'created_at': {'$gte': start_datetime, '$lt': end_datetime}}

# Initialize logging
script_name = os.path.splitext(os.path.basename(__file__))[0]
log_file_name = f"{script_name}.log"
logger = log_config(log_file_name)

logger.info('STARTING NEW RUN\n')

with MongoClient(src_uri) as src_client, MongoClient(dest_uri) as dest_client:
    src_db = src_client['award_shopper']
    dest_db = dest_client['awayzDB']

    for hotel in mongo_hotels:
        for table in mongo_tables:
            table_name = f"{table}_{hotel}"
            logger.info(f"Transferring data for {table_name}")
            
            src_col = src_db[table_name]
            dest_col = dest_db[table_name]
            
            query_filter = date_filter if 'hotel_calendar' in table_name else None
            logger.info(f"query = {query_filter}")
            
            try:
                transfer_data(logger, src_col, dest_col, query_filter=query_filter)
            except Exception as e:
                logger.error(f"Failed to transfer data for {table_name}")
            logger.info(f"Data transfer complete for {table_name}!")