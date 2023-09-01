# output_table_dict = (
#         {
#         'table_name':'hotel_cash',
#         'table_columns':
#             {    # Cash columns
#             'hotel_group': 'TEXT',
#             'hotel_name': 'TEXT',
#             'date': 'DATE',
#             'cash_value': 'NUMERIC',
#             'currency': 'TEXT',
#             'created_at': 'TIMESTAMP',
#             'award_category': 'TEXT',
#             'hotel_name_key': 'TEXT',
#             'hotel_id': 'TEXT',
#             '_id': 'TEXT'
#             },
#         },
#         {
#     'table_name':'hotel_points', 
#     'table_columns': {    # Points columns
#             'hotel_group': 'TEXT',
#             'hotel_name': 'TEXT',
#             'date': 'DATE',
#             'points': 'NUMERIC',
#             'created_at': 'TIMESTAMP',
#             'award_category': 'TEXT',
#             'points_level': 'TEXT',
#             'hotel_name_key': 'TEXT',
#             'hotel_id': 'TEXT',
#             '_id': 'TEXT'
#         }
#         }
# )

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

input_output_settings = {
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