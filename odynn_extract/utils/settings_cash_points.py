from bson.objectid import ObjectId

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

input_output_dict = {
    'cash': {
        'input_tables': {
            'hotel_calendar_cash_hilton',
            'hotel_calendar_cash_hyatt',
            'hotel_calendar_cash_ihg',
            'hotel_calendar_cash_marriott',
            'archived_hotel_calendar_cash_hilton',
            'archived_hotel_calendar_cash_hyatt',
            'archived_hotel_calendar_cash_ihg',
            'archived_hotel_calendar_cash_marriott',       
            },
        'output_table': {
            'table_name': 'hotel_cash',
            'table_columns': {
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
        }
    },
    'points': {
        'input_tables': {
            'hotel_calendar_hilton',
            'hotel_calendar_hyatt',
            'hotel_calendar_ihg',
            'hotel_calendar_marriott', 
            'archived_hotel_calendar_hilton',
            'archived_hotel_calendar_hyatt',
            'archived_hotel_calendar_ihg',
            'archived_hotel_calendar_marriott', 
            },
        'output_table': {
            'table_name': 'hotel_points',
            'table_columns': {
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
        }
    }
}

def query_cash_points(input_table, start_id):
    is_archived = True if input_table.split("_")[0] == 'archived' else False
    hotel_group = input_table.split('_')[-1]
    
    # Handle Hyatt edge case for new york
    if not(is_archived) and hotel_group == 'hyatt':
        new_york_str = 'New York'
    else:
        new_york_str = 'new-york'
    
    # Initialize query to restrict to new-york
    query = {'city': new_york_str}
    
    # If last_id is defined, append an _id filter to the query dictionary
    if start_id:
        query['_id'] = {'$lt': ObjectId(start_id)}
        
    return query