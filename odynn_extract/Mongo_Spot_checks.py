import odynn_pipeline_cash
from pymongo import MongoClient, DESCENDING, ASCENDING
import pandas as pd
from bson.objectid import ObjectId
import time
from datetime import datetime

collection_names = [
    'archived_hotel_calendar_cash_hilton',
    'archived_hotel_calendar_cash_hyatt',
    'archived_hotel_calendar_cash_ihg',
    'archived_hotel_calendar_cash_marriott', 
]

input_uri = odynn_pipeline_cash.input_uri
client = MongoClient(input_uri)
db = client['award_shopper']

# collection_name = 'archived_hotel_calendar_cash_ihg'
collection_name = 'archived_hotel_calendar_cash_hilton'
hotel_name_key_target = 'intercontinental-hotels-new-york-barclay'

#### SCRIPT ###
start_time = datetime.now()
print("Starting script:", start_time)

# Find the document with the minimum created_at for the specified hotel_name_key
collection = db[collection_name]
result = collection.find(
    {"hotel_name_key": hotel_name_key_target},
    {"created_at": 1}
).sort("created_at", 1).limit(1)

min_created_at = next(result, {}).get('created_at', None)

if min_created_at:
    print(f"The minimum created_at for hotel_name_key '{hotel_name_key_target}' is {min_created_at}")
else:
    print(f"No records found for hotel_name_key '{hotel_name_key_target}'")

end_time = datetime.now()
print("Finished script in {:.2f} seconds at {}".format((end_time - start_time).total_seconds(),end_time))