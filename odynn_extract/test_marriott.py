import odynn_pipeline_cash
from pymongo import MongoClient, DESCENDING, ASCENDING
import pandas as pd
from bson.objectid import ObjectId

input_uri = odynn_pipeline_cash.input_uri
client = MongoClient(input_uri)
db = client['award_shopper']

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# Script to determine total number of cash_values, accross all regions and hotel_name_keys
collection_names = [
    'archived_hotel_calendar_cash_hilton',
    'archived_hotel_calendar_cash_hyatt',
    'archived_hotel_calendar_cash_ihg',
    'archived_hotel_calendar_cash_marriott', 
]

result_df = pd.DataFrame(columns=['collection_name', 'row_count', 'unique_rows', 'unique_hotel_name_keys', 'unique_cities'])

for collection_name in collection_names:
    collection = db[collection_name]
    
    row_count = collection.count_documents({})
    
    # Group data by hotel_name_key, date, and date(created_at_date)
    pipeline = [
        {
            "$match": {
                "created_at": {"$type": 9},  # Only consider documents where created_at is a date, timestamp, or timestamp with timezone
                "cash_value": {"$gt": 1} # filter by cash_value > 1
            }
        },
        { # Dedupe by hotel_name_key, date(created_at), and date (i.e., date_booking)
            "$group": {
                "_id": {
                    "hotel_name_key": "$hotel_name_key",
                    "created_at": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$created_at"
                        }
                    },
                    "date": "$date"
                }
            }
        },
        {
            "$count": "unique_rows"
        }
    ]
    

    try:
        result = collection.aggregate(pipeline, allowDiskUse=True)
        result_list = list(result)
        unique_rows = result_list[0]['unique_rows'] if result_list else 0
        
        # Aggregate stats
        unique_hotel_name_keys = len(collection.distinct("hotel_name_key"))
        unique_cities = len(collection.distinct("city"))
        
        new_row = {
            'collection_name': collection_name,
            'row_count': row_count,
            'unique_rows': unique_rows,
            'unique_hotel_name_keys': unique_hotel_name_keys,
            'unique_cities': unique_cities,
        }

        result_df = result_df.append(new_row, ignore_index=True)
        
    except Exception as e:
        print(f"An error occurred processing {collection_name}: {e}")
        continue

print(result_df)

result_df.to_csv('unique_observations.csv', index=False)


# def count_cash_value_documents(input_uri, collection_name):
#     client = MongoClient(input_uri)
#     collection = client.award_shopper[collection_name]

#     total_count = collection.count_documents({})
#     cash_value_dict_count = collection.count_documents({'cash_value': {'$type': 'object'}})

#     client.close()

#     print(f'Total number of documents in {collection_name}: {total_count}')
#     print(f'Number of documents where cash_value is a dictionary: {cash_value_dict_count}')

# def most_recent_date_per_hotel(input_uri, collection_name):
#     client = MongoClient(input_uri)
#     collection = client.award_shopper[collection_name]
    
#     pipeline = [
#         {"$match": {"city": "new-york"}},
#         {"$group": {
#             "_id": {
#                 "hotel_name_key": "$hotel_name_key",
#                 "city": "$city"
#             },
#             "most_recent_date": {"$max": "$created_at"},
#             "min_created_at": {"$min": "$created_at"},
#             "count": {"$sum": 1}
#         }}
#     ]
    
#     result = collection.aggregate(pipeline)
#     df = pd.DataFrame(list(result))
#     client.close()

#     # Check that each dictionary in the _id column has all the expected keys
#     df = df[df['_id'].apply(lambda x: all(key in x for key in ['hotel_name_key', 'city']))]

#     # Split the _id column into separate columns
#     df[['hotel_name_key', 'city']] = df['_id'].apply(pd.Series)
#     df.drop('_id', axis=1, inplace=True)

#     return df

# def get_row_counts(db, client, collection_names):
#     data = []

#     for collection_name in collection_names:
#         print(f'Counting rows in {collection_name}')
        
#         collection = db[collection_name]
#         total_rows = collection.count_documents({})
#         data.append([collection_name, total_rows])
#         print(f'{total_rows} rows in {collection_name}')

#     df = pd.DataFrame(data, columns=['collection_name', 'total_rows'])
#     client.close()
    
#     return df

# define the start and end of the week
# start_date = datetime.datetime(2023, 5, 8)
# end_date = start_date + datetime.timedelta(days=6)


### QA from a specific _id
# start_id = ObjectId('636b9486bd5eaf02ca7affbd')
# collection = db['archived_hotel_calendar_cash_marriott']

# #Query the collection
# #cursor = collection.find({'_id': {'$lt': start_id}, 'city': 'new-york'}).sort('_id', DESCENDING).limit(10000)
# cursor = collection.find({'_id': {'$lt': start_id}, 'city': 'new-york'}).sort('_id', ASCENDING).limit(10000)
# df = pd.DataFrame(list(cursor))
# df.to_csv('marriott_qa.csv', index=False)

# breakpoint()



# ### SCRIPT - Spot check raw rows ###
# results = db.archived_hotel_calendar_marriott.find({
#     'hotel_name_key': 'renaissance-new-york-times-square-hotel',
#     '$or': [{'cash_value': {'$ne': None}}, {'points': {'$ne': None}}],
#     'created_at': {'$gte': start_date, '$lte': end_date}
# })

# df = pd.DataFrame(list(results))

# print(df)

# df.to_csv('spot_check.csv', index=False)


# # For Awards QA
# collection_names = [
#     'archived_hotel_calendar_hilton',
#     'archived_hotel_calendar_hyatt',
#     'archived_hotel_calendar_ihg',
#     'archived_hotel_calendar_marriott', 
# ]

# Script to count rows:

# df = get_row_counts(db, client, collection_names)
# print(df)
# df.to_csv('input_table_rows.csv', index=False)


#count_cash_value_documents(input_uri, collection_name='archived_hotel_calendar_cash_marriott')

# df = most_recent_date_per_hotel(input_uri, collection_name='archived_hotel_calendar_cash_hyatt')
# breakpoint()
# df.to_csv('output.csv', index=False)

# Find the next 1000 records after X


