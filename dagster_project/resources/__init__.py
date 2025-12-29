"""
Resources for Dagster pipeline.
Includes database connections and configurations.
"""
from dagster import ConfigurableResource
import duckdb
from pathlib import Path

class DuckDBResource(ConfigurableResource):
    """
    DuckDB connection resource.
    Provides a consistent way to connect to the database across assets.
    """
    database_path: str = "data/ecommerce.duckdb"
    
    def get_connection(self):
        """Return a DuckDB connection."""
        return duckdb.connect(self.database_path)
    
    def execute_query(self, query: str):
        """Execute a query and return results as pandas DataFrame."""
        with self.get_connection() as conn:
            return conn.execute(query).df()
    
    def get_table_row_count(self, schema: str, table: str) -> int:
        """Get row count for a specific table."""
        query = f"SELECT COUNT(*) as count FROM {schema}.{table}"
        result = self.execute_query(query)
        return result['count'].iloc[0]

# Example usage in assets:
# from dagster_project.resources import DuckDBResource
# 
# @asset
# def my_asset(duckdb: DuckDBResource):
#     df = duckdb.execute_query("SELECT * FROM bronze.customers LIMIT 10")
#     return df