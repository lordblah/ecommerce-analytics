"""E-Commerce Analytics Pipeline"""
import os
import pandas as pd
import duckdb
from pathlib import Path

from dagster import (
    asset,
    Definitions,
    AssetExecutionContext,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
)
from dagster_dbt import DbtCliResource, dbt_assets

# ==================== BRONZE ASSETS (defined here directly) ====================

BRONZE_DIR = "C:/Users/jennifer/ecommerce-analytics/data/bronze"
DB_PATH = "C:/Users/jennifer/ecommerce-analytics/data/ecommerce.duckdb"

@asset(group_name="bronze", compute_kind="python")
def bronze_customers(context: AssetExecutionContext):

    """Load customers from CSV"""
    df = pd.read_csv(f"{BRONZE_DIR}/customers.csv")
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("DROP TABLE IF EXISTS bronze.customers")
        conn.execute("CREATE TABLE bronze.customers AS SELECT * FROM df")
    context.log.info(f"✓ Loaded {len(df)} customers")
    return len(df)

@asset(group_name="bronze", compute_kind="python")
def bronze_products(context: AssetExecutionContext):
    """Load products from CSV"""
    df = pd.read_csv(f"{BRONZE_DIR}/products.csv")
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("DROP TABLE IF EXISTS bronze.products")
        conn.execute("CREATE TABLE bronze.products AS SELECT * FROM df")
    context.log.info(f"✓ Loaded {len(df)} products")
    return len(df)

@asset(group_name="bronze", compute_kind="python")
def bronze_orders(context: AssetExecutionContext):
    """Load orders from CSV"""
    df = pd.read_csv(f"{BRONZE_DIR}/orders.csv")
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("DROP TABLE IF EXISTS bronze.orders")
        conn.execute("CREATE TABLE bronze.orders AS SELECT * FROM df")
    context.log.info(f"✓ Loaded {len(df)} orders")
    return len(df)

@asset(group_name="bronze", compute_kind="python")
def bronze_order_items(context: AssetExecutionContext):
    """Load order items from CSV"""
    df = pd.read_csv(f"{BRONZE_DIR}/order_items.csv")
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("DROP TABLE IF EXISTS bronze.order_items")
        conn.execute("CREATE TABLE bronze.order_items AS SELECT * FROM df")
    context.log.info(f"✓ Loaded {len(df)} order items")
    return len(df)

# ==================== DBT ASSETS ====================

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"

@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def ecommerce_dbt_assets(context, dbt: DbtCliResource):
    """dbt models (Silver & Gold)"""
    yield from dbt.cli(["build"], context=context).stream()

# ==================== JOBS ====================

daily_sales_pipeline = define_asset_job(
    name="daily_sales_pipeline",
    description="Complete pipeline",
    selection=AssetSelection.all(),
)

# ==================== DEFINITIONS ====================

defs = Definitions(
    assets=[
        bronze_customers,
        bronze_products,
        bronze_orders,
        bronze_order_items,
        ecommerce_dbt_assets,
    ],
    jobs=[daily_sales_pipeline],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)