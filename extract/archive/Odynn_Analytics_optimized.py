import pandas as pd
import numpy as np
from pymongo import MongoClient
#from urllib.parse import urlparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar, time

# MongoDB connetion details
uri = 'mongodb://data_reader:READ%23%23%24Awayzasdf33d!@44.212.136.220:27017/award_shopper?readPreference=primary&ssl=false&directConnection=true&authMechanism=DEFAULT&authSource=award_shopper'
db_name = 'award_shopper'

start_time = time.time()

# create a MongoDB client
client = MongoClient(uri)

# Define the filter criteria
filter_criteria = {
    "city": "new-york",
    "$expr": {
        "$eq": [{"$dayOfWeek": "$created_at"}, 3]  # 1 for Sunday, 2 for Monday, ..., 7 for Saturday
    }
}

# Define collection (table) to query
collection_name = 'archived_hotel_calendar_cash_hilton'

# Set up MongoDB connection and retrieve specified collection (table)
collection = client.get_database(db_name).get_collection(collection_name)
    


# Define the projection fields and date conversion
projection = {
    "date": {"$dateFromString": {"dateString": "$date", "format":"%Y-%m-%d"}},
    "cash_value_amount": "$cash_value.amount",
    "created_at": "created_at",
    "hotel_name_key": "hotel_name_key",
}

cursor = collection.aggregate([
    {"$match": filter_criteria},
    {"$project": projection}
])

df = pd.DataFrame(list(cursor))
print(df)

client.close()

## Print the runtime ##
end_time = time.time()
runtime = end_time - start_time
print("Runtime: {:.2f} seconds".format(runtime))

breakpoint()



## Analysis Setup ##
# Get the current date and time
# now = datetime.now()

# # Calculate the end of last month
# end_date = datetime(now.year, now.month, 1) - timedelta(days=1)
# last_month_start = datetime(end_date.year, end_date.month, 1)

# # Subtract 16 months from the end of last month
# start_date = last_month_start - relativedelta(months=16)


# match1 = {"location": "new_york"}
# group1 = {"_id": "$date", "count":{"$sum": 1}}

# pipeline1 = [
#     {"$match": match1},
#     {"$group": group1}
# ]

# result = list(collection.aggregate(pipeline1))

# # Print the number of rows for each date
# for item in result:
#     date = item["_id"]
#     count = item["count"]
#     print("Date:", date, "Count:", count)


# pipeline = [
#     {
#         "$match": {
#             "city": "new-york"
#         }
#     },
#     {
#         "$addFields": {
#             "convertedDate": {
#                 "$toDate": "$date"
#             },
#             "convertedCreatedAt": {
#                 "$toDate": "$created_at"
#             }
#         }
#     },
#     {
#         "$group": {
#             "_id": {
#                 "date_year": { "$year": "$convertedDate" },
#                 "date_month": { "$month": "$convertedDate" },
#                 "created_year": { "$year": "$convertedCreatedAt" },
#                 "created_month": { "$month": "$convertedCreatedAt" }
#             },
#             "count": { "$sum": 1 },
#             "average_price": { "$avg": "$price" }
#         }
#     },
#     {
#         "$sort": { "_id.date_year": 1, "_id.date_month": 1 }
#     }
# ]

# result = list(collection.aggregate(pipeline))

# result = list(collection.aggregate(pipeline))

# for item in result:
#     date_year = item["_id"]["date_year"]
#     date_month = item["_id"]["date_month"]
#     created_year = item["_id"]["created_year"]
#     created_month = item["_id"]["created_month"]
#     count = item["count"]
#     average_price = item["average_price"]

#     print("Date: {}-{}, Created: {}-{}, Count: {}, Average Price: {:.2f}".format(date_year, date_month, created_year, created_month, count, average_price))