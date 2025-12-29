"""Bronze layer assets - CSV to DuckDB ingestion"""
import os
import pandas as pd
import duckdb
from dagster import asset, AssetExecutionContext

BRONZE_DIR = "data/bronze"
DB_PATH = "data/ecommerce.duckdb"

@asset(
    group_name="bronze",
    description="Load customers from CSV",
    compute_kind="python",
)
def bronze_customers(context: AssetExecutionContext):
    """Load customer data into DuckDB"""
    csv_path = os.path.join(BRONZE_DIR, "customers.csv")
    context.log.info(f"Loading {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("DROP TABLE IF EXISTS bronze.customers")
        conn.execute("CREATE TABLE bronze.customers AS SELECT * FROM df")
    
    context.log.info(f"✓ Loaded {len(df)} customers")
    return len(df)

@asset(
    group_name="bronze",
    description="Load products from CSV",
    compute_kind="python",
)
def bronze_products(context: AssetExecutionContext):
    """Load product data into DuckDB"""
    csv_path = os.path.join(BRONZE_DIR, "products.csv")
    context.log.info(f"Loading {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("DROP TABLE IF EXISTS bronze.products")
        conn.execute("CREATE TABLE bronze.products AS SELECT * FROM df")
    
    context.log.info(f"✓ Loaded {len(df)} products")
    return len(df)

@asset(
    group_name="bronze",
    description="Load orders from CSV",
    compute_kind="python",
)
def bronze_orders(context: AssetExecutionContext):
    """Load order data into DuckDB"""
    csv_path = os.path.join(BRONZE_DIR, "orders.csv")
    context.log.info(f"Loading {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("DROP TABLE IF EXISTS bronze.orders")
        conn.execute("CREATE TABLE bronze.orders AS SELECT * FROM df")
    
    context.log.info(f"✓ Loaded {len(df)} orders")
    return len(df)

@asset(
    group_name="bronze",
    description="Load order items from CSV",
    compute_kind="python",
)
def bronze_order_items(context: AssetExecutionContext):
    """Load order items data into DuckDB"""
    csv_path = os.path.join(BRONZE_DIR, "order_items.csv")
    context.log.info(f"Loading {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("DROP TABLE IF EXISTS bronze.order_items")
        conn.execute("CREATE TABLE bronze.order_items AS SELECT * FROM df")
    
    context.log.info(f"✓ Loaded {len(df)} order items")
    return len(df)