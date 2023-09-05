import os, logging, traceback
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
from pymongo import MongoClient

load_dotenv() # lond environmental variables

def extract_mongodb(mongo_database, input_table, query, chunk_size, sort_column, sort_order, logger):
    env_str = "MONGO_URI"
    uri = os.environ.get(env_str)
    
    try:
        with MongoClient(uri) as client:
            collection = client[mongo_database][input_table]
            cursor = collection.find(query).sort([(sort_column, sort_order)]).limit(chunk_size)
            df = pd.DataFrame(list(cursor))
        return df
    except Exception as e:
        logger.error(f'Error extracting from {input_table} via {query} sorted {sort_column} by {sort_order}. {e}')
        return None


class PostgresInserter:
    def __init__(self):
        env_str = "POSTGRESQL_URI"
        self.uri = os.environ.get(env_str)
        if self.uri is None:
            raise ValueError(f"{env_str} not found in environment variables")
        self.engine = create_engine(self.uri)
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def create_table(self, table_name, columns_dict, logger):
        # Define the columns and generate "create table" query
        column_definitions = [f"{column} {data_type}" for column, data_type in columns_dict.items()]
        # column_definitions.append("id SERIAL PRIMARY KEY")
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
        try:
            with self.engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT")
                logger.info(f"Executing query: {create_table_query}")
                connection.execute(text(create_table_query))
            return list(columns_dict.keys()) # Return list of column names
        except Exception as e:
            print(f"Error executing query {create_table_query} {e}")
            return None
            
    def insert_postgres(self, df: pd.DataFrame, table_name: str, logger, helper_columns=None, column_order=None):
        if df is None:
            logger.warning(f"No data to save to {table_name}. Skipping.")
            return
        # Add optional helper_columns via dictionary
        if helper_columns is not None:
            for column, value in helper_columns.items():
                df[column] = value
                
        # Standardize column order if provided
        if column_order is not None:
            df = df[column_order]
            
        # Save DataFrame to PostgreSQL table
        try:
            df.to_sql(table_name, self.engine, index=False, if_exists='append')
            logger.info(f"Saved {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error saving data to {table_name}: \n{traceback.format_exc()}")
            
    def start_run(self, run_name, prefix, logger):
        run_dt = datetime.utcnow()
        table_name = f"{prefix}run"
        insert_query = text(f"INSERT INTO {table_name} (run_dt, run_name) VALUES (:run_dt, :run_name) RETURNING run_id;")
        try:
            with self.engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT") # automatically commit insertions
                result = connection.execute(insert_query, {'run_dt': run_dt, 'run_name': run_name})
                run_id = result.fetchone()[0]
                return run_id
        except Exception as e:
            logger.error(f"Error adding run {run_id} of {run_name} into {table_name}: {e} \n {insert_query}")
        
    def close(self):
        self.engine.dispose()  # Close the database engine