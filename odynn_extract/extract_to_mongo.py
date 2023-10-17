from datetime import datetime
from itertools import islice
from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv

def get_id_range(src_col, start_datetime, end_datetime):
    min_doc = src_col.find({'created_at': {'$gte': start_datetime}}).sort('_id', 1).limit(1)
    max_doc = src_col.find({'created_at': {'$lt': end_datetime}}).sort('_id', -1).limit(1)
    
    min_id = min_doc[0]['_id'] if min_doc.count() > 0 else None
    max_id = max_doc[0]['_id'] if max_doc.count() > 0 else None
    
    return min_id, max_id

def batch_iterator(iterator, batch_size):
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            return
        yield batch

def transfer_data(logger, src_col, dest_col, query_filter=None, batch_size=10000):
    # Clear the destination collection
    dest_col.delete_many({})
    
    if query_filter:
        min_id, max_id = get_id_range(src_col, query_filter.get('created_at').get('$gte'), query_filter.get('created_at').get('$lt'))
        
        # Replace query_filter to only include _id range
        if min_id and max_id:
            query_filter = {'_id': {'$gte': min_id, '$lte': max_id}}

    cursor = src_col.find(query_filter)
    
    success_count = 0
    error_count = 0
    
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

# Load environment variables
load_dotenv()
src_uri = os.getenv("MONGO_URI")
dest_uri = os.getenv("OUTPUT_MONGO_URI")

mongo_hotels = ['hilton', 'hyatt','ihg', 'marriott', 'accor']
mongo_tables = ['hotel_calendar', 'hotel_calendar_cash', 'hotel_directory_templates', 'archived_hotel_calendar', 'archived_hotel_calendar_cash']

start_datetime = datetime(2023, 10, 1, 0, 0, 0)
end_datetime = datetime(2023, 10, 2, 0, 0, 0)

date_filter = {'created_at': {'$gte': start_datetime, '$lt': end_datetime}}

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            
            transfer_data(logger, src_col, dest_col, query_filter=query_filter)
            logger.info(f"Data transfer complete for {table_name}!")