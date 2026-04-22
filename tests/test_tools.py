"""
Tests for custom DuckDB tools — ecommerce-specific.
Run with: pytest tests/ -v
"""

import pytest


@pytest.fixture
def ecommerce_db(tmp_path):
    """Create a temp DuckDB with medallion-style ecommerce tables."""
    import duckdb

    db_path = str(tmp_path / "test_ecommerce.duckdb")
    conn = duckdb.connect(db_path)

    # Bronze layer — raw
    conn.execute("""
        CREATE TABLE bronze_orders (
            id INTEGER,
            user_id INTEGER,
            order_date DATE,
            status VARCHAR,
            _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO bronze_orders (id, user_id, order_date, status) VALUES
        (1, 100, '2024-01-15', 'completed'),
        (2, 101, '2024-01-16', 'completed'),
        (3, 100, '2024-01-17', 'returned'),
        (4, 102, '2024-01-17', 'completed'),
        (4, 102, '2024-01-17', 'completed')  -- intentional duplicate
    """)

    # Silver layer — cleaned
    conn.execute("""
        CREATE TABLE silver_orders AS
        SELECT DISTINCT
            id AS order_id,
            user_id AS customer_id,
            order_date,
            status,
            _loaded_at
        FROM bronze_orders
    """)

    # Gold layer — aggregated
    conn.execute("""
        CREATE TABLE gold_customer_orders AS
        SELECT
            customer_id,
            COUNT(*) AS total_orders,
            COUNT(*) FILTER (WHERE status = 'completed') AS completed_orders,
            MIN(order_date) AS first_order_date,
            MAX(order_date) AS last_order_date
        FROM silver_orders
        GROUP BY customer_id
    """)

    conn.close()
    return db_path


@pytest.mark.anyio
async def test_query_warehouse(ecommerce_db):
    from src.tools.duckdb_tools import query_warehouse

    result = await query_warehouse({
        "sql": "SELECT COUNT(*) AS cnt FROM gold_customer_orders",
        "database_path": ecommerce_db,
    })

    assert not result.get("isError")
    assert "3" in result["content"][0]["text"]  # 3 unique customers


@pytest.mark.anyio
async def test_list_tables(ecommerce_db):
    from src.tools.duckdb_tools import list_tables

    result = await list_tables({"database_path": ecommerce_db})
    text = result["content"][0]["text"]

    assert "bronze_orders" in text
    assert "silver_orders" in text
    assert "gold_customer_orders" in text


@pytest.mark.anyio
async def test_describe_table(ecommerce_db):
    from src.tools.duckdb_tools import describe_table

    result = await describe_table({
        "table_name": "gold_customer_orders",
        "database_path": ecommerce_db,
    })
    text = result["content"][0]["text"]

    assert "customer_id" in text
    assert "total_orders" in text


@pytest.mark.anyio
async def test_data_quality_check_clean(ecommerce_db):
    from src.tools.duckdb_tools import data_quality_check

    result = await data_quality_check({
        "table_name": "gold_customer_orders",
        "primary_key": "customer_id",
        "database_path": ecommerce_db,
    })
    text = result["content"][0]["text"]

    assert "No duplicates" in text
    assert "Total rows" in text


@pytest.mark.anyio
async def test_data_quality_check_duplicates(ecommerce_db):
    from src.tools.duckdb_tools import data_quality_check

    # bronze_orders has a duplicate id=4
    result = await data_quality_check({
        "table_name": "bronze_orders",
        "primary_key": "id",
        "database_path": ecommerce_db,
    })
    text = result["content"][0]["text"]

    assert "duplicates found" in text.lower() or "Duplicate" in text


@pytest.mark.anyio
async def test_query_error_handling(ecommerce_db):
    from src.tools.duckdb_tools import query_warehouse

    result = await query_warehouse({
        "sql": "SELECT * FROM nonexistent_table",
        "database_path": ecommerce_db,
    })

    assert result.get("isError") is True


@pytest.mark.anyio
async def test_empty_result(ecommerce_db):
    from src.tools.duckdb_tools import query_warehouse

    result = await query_warehouse({
        "sql": "SELECT * FROM gold_customer_orders WHERE customer_id = -999",
        "database_path": ecommerce_db,
    })

    assert "0 rows" in result["content"][0]["text"]
