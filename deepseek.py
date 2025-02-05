import pandas as pd
from sqlalchemy import create_engine, exc
import logging
from tqdm import tqdm  # For progress bar (optional)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection details
conn_string = (
    "mssql+pyodbc://@SARANGS-X1-G10\\SQLEXPRESS/tecTFQ"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

# List of CSV files to load
files = ['artist', 'canvas_size', 'image_link', 'museum_hours', 'museum', 'product_size', 'subject', 'work']

# Base directory for CSV files
base_dir = r'D:\SQL\techTFQ'

try:
    # Create database engine
    engine = create_engine(conn_string)
    logger.info("Database engine created successfully.")

    # Test the connection
    with engine.connect() as conn:
        logger.info("Connected to the database successfully.")

    # Loop through each file and load into the database
    for file in tqdm(files, desc="Loading files"):  # tqdm for progress bar
        try:
            # Read CSV file
            file_path = f"{base_dir}\\{file}.csv"
            logger.info(f"Reading file: {file_path}")
            df = pd.read_csv(file_path)

            # Load data into SQL
            logger.info(f"Loading data into table: {file}")
            df.to_sql(
                name=file,          # Table name
                con=engine,         # Database connection
                if_exists='replace',# Replace table if it exists
                index=False,        # Do not write DataFrame index as a column
                method='multi',     # Use multi-row inserts for better performance
                chunksize=1000      # Insert in chunks for large datasets
            )
            logger.info(f"Successfully loaded data into table: {file}")

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
        except pd.errors.EmptyDataError:
            logger.error(f"File is empty: {file_path}")
        except exc.SQLAlchemyError as e:
            logger.error(f"Database error while loading {file}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while processing {file}: {e}")

except exc.SQLAlchemyError as e:
    logger.error(f"Failed to connect to the database: {e}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
finally:
    # Dispose of the engine to close all connections
    engine.dispose()
    logger.info("Database engine disposed. All connections closed.")