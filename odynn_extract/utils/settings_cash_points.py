from bson.objectid import ObjectId
import pandas as pd

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

column_order_cash = 1

def clean_cash(df, helper_columns, logger):
     # Check if cash_value exists
    try:
        if 'cash_value' in df.columns:
            # Keep only rows where cash_value exists as a dictionary
            df = df[df['cash_value'].apply(lambda x: isinstance(x, dict))]
            # Add helper columns
            for column, value in helper_columns.items():
                df[column] = value

            # Reorder and drop unneeded columns
            df = df.reindex(columns=column_order_cash)
            # Avoid dupe 'currecny' col after flattening cash_value dictionary
            df = df.drop(columns=['currency'])
            
            if df.empty:
                df = None
                return None
            
            # Convert date to datetime and ignore errors
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            # Drop rows where 'date' is NaT
            df = df.dropna(subset=['date'])
            
            # Flatten 'cash_value' dictionary into distinct columns ('records' specifies the format)
            df = pd.json_normalize(df.to_dict('records'))
            
            # Rename flattened columns
            df = df.rename(columns={'cash_value.amount': 'cash_value', 'cash_value.currency': 'currency'})
            
            # Convert data types for insertion for postgres insertion
            df['cash_value'] = df['cash_value'].apply(pd.to_numeric, errors='coerce')
            df['_id'] = df['_id'].astype(str)
            
            # Standardize column order
            df = df[column_order_cash]
            return df
        
    except Exception as e:
        logger.error(f'Error parsing cash {df} with {helper_columns}. {e}')
        return None

def clean_points(df, helper_columns, logger):
    clean_df = None
    print("clean_points function not finished")
    return clean_df
