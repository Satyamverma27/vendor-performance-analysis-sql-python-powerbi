import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# --- Create logs directory if it doesn't exist ---
if not os.path.exists('logs'):
    os.makedirs('logs')

# --- Configure Logging ---
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# --- Initialize Database Engine ---
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''
    This function will ingest the dataframe into the database table.
    '''
    try:
        logging.info(f"Ingesting {table_name} into database...")
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully ingested {table_name}")
    except Exception as e:
        logging.error(f"Error ingesting {table_name}: {e}")
        raise e

def load_raw_data():
    '''
    This function will load the CSVs as dataframes and ingest them into the db.
    '''
    start = time.time()

    if not os.path.exists('data'):
        logging.error("Data folder not found!")
        print("Error: 'data' folder not found.")
        return

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            try:
                file_path = os.path.join('data', file)
                df = pd.read_csv(file_path)

                # Create a table name (remove the .csv extension)
                table_name = file.replace('.csv', '')

                logging.info(f'Processing file: {file}')
                ingest_db(df, table_name, engine)

            except Exception as e:
                logging.error(f"Failed to process {file}: {e}")

    end = time.time()
    total_time = (end - start) / 60

    logging.info('----------------Ingestion Complete----------------')   
    logging.info(f'Total time taken: {total_time:.2f} minutes')
    print("Ingestion Complete.")

if __name__ == '__main__':
    load_raw_data()
