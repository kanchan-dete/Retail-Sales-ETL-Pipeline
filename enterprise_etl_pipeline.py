import logging
import pandas as pd
import sqlite3

# Configure Logging for Pipeline Monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RetailETLPipeline:
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = None
        self.cursor = None

    def connect_db(self):
        """Establish connection to the SQLite Data Warehouse file."""
        try:
            self.connection = sqlite3.connect(self.db_name)
            self.cursor = self.connection.cursor()
            logger.info(f"Successfully connected to SQLite database: {self.db_name}")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def extract(self, file_path):
        """Extract raw data from source storage (simulating batch CSV ingestion)."""
        logger.info(f"EXTRACT: Reading raw data from source: {file_path}")
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Extracted {len(df)} records successfully.")
            return df
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            raise

    def transform(self, df):
        """Transform data: Clean nulls, enforce schema, and handle aggregations."""
        logger.info("TRANSFORM: Starting data cleaning and preprocessing pipeline.")
        
        initial_count = len(df)
        
        # 1. Handle Missing Values / Data Imputation
        df['customer_name'] = df['customer_name'].fillna('Anonymous')
        df['product'] = df['product'].fillna('Uncategorized Product')
        
        # 2. Schema Enforcement & Data Type Casting
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df = df.dropna(subset=['order_date']) # Drop rows with invalid unrecoverable dates
        
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
        df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce').fillna(0.0)

        # 3. Feature Engineering (Calculating derived metrics)
        df['total_sales_amount'] = df['quantity'] * df['price_per_unit']
        
        # 4. Deduplication
        df = df.drop_duplicates()
        
        final_count = len(df)
        logger.info(f"TRANSFORM Complete. Cleaned {initial_count - final_count} corrupted/duplicate records.")
        return df

    def load(self, df, table_name):
        """Load transformed data into SQLite Data Warehouse table."""
        logger.info(f"LOAD: Inserting {len(df)} rows into table '{table_name}'.")
        try:
            # Create production table with proper data engineering schema constraints
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    order_id INTEGER PRIMARY KEY,
                    customer_name TEXT,
                    product TEXT,
                    quantity INTEGER,
                    price_per_unit REAL,
                    order_date TEXT,
                    total_sales_amount REAL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Insert or replace records into SQLite
            records = [
                (
                    int(row['order_id']), 
                    str(row['customer_name']), 
                    str(row['product']), 
                    int(row['quantity']), 
                    float(row['price_per_unit']), 
                    row['order_date'].strftime('%Y-%m-%d'), 
                    float(row['total_sales_amount'])
                ) 
                for _, row in df.iterrows()
            ]

            self.cursor.executemany(f"""
                INSERT OR REPLACE INTO {table_name} 
                (order_id, customer_name, product, quantity, price_per_unit, order_date, total_sales_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """, records)

            self.connection.commit()
            logger.info("LOAD Complete. Data successfully committed to SQLite Data Warehouse.")

        except Exception as e:
            logger.error(f"Error loading data into SQLite: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    def close(self):
        """Clean up database connections."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed cleanly.\n")

# Pipeline Execution Orchestration
if __name__ == "__main__":
    # SQLite Database file name (will be created automatically in your folder)
    db_file = 'retail_warehouse.db'

    # Creating a dummy CSV source file on the fly for testing
    dummy_csv = 'raw_retail_sales.csv'
    sample_df = pd.DataFrame({
        'order_id': [5001, 5002, 5003, 5004, 5005],
        'customer_name': ['Amit', None, 'Sneha', 'Rahul', 'Pooja'],
        'product': ['Server Rack', 'SSD Drive', None, 'Router', 'Switch'],
        'quantity': [2, 5, 1, 3, 10],
        'price_per_unit': [15000.0, 4500.0, 2500.0, 3000.0, 800.0],
        'order_date': ['2026-06-01', '2026-06-01', 'Bad_Date', '2026-06-02', '2026-06-03']
    })
    sample_df.to_csv(dummy_csv, index=False)

    # Initialize and run pipeline
    pipeline = RetailETLPipeline(db_file)
    try:
        pipeline.connect_db()
        raw_data = pipeline.extract(dummy_csv)
        cleaned_data = pipeline.transform(raw_data)
        pipeline.load(cleaned_data, 'dim_retail_sales')
    except Exception as e:
        logger.error(f"Pipeline Execution Failed: {e}")
    finally:
        pipeline.close()