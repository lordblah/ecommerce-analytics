"""
Custom DuckDB Tools for Ecommerce Analytics
=============================================
In-process MCP server that gives agents the ability to query the
ecommerce DuckDB warehouse, inspect schemas, and run data quality checks.

Tools run in the same Python process — no subprocess overhead.
All connections are read-only to prevent agent mutations.
"""

from __future__ import annotations

import os
from claude_agent_sdk import tool, create_sdk_mcp_server


# Default path — override via ECOMMERCE_DUCKDB_PATH env var
DEFAULT_DB_PATH = os.getenv("ECOMMERCE_DUCKDB_PATH", "ecommerce.duckdb")


@tool(
    name="query_warehouse",
    description=(
        "Run a read-only SQL query against the ecommerce DuckDB warehouse. "
        "Returns results as a markdown table. Use for data exploration, "
        "metric validation, and ad-hoc analysis. The warehouse uses medallion "
        "architecture: bronze (raw), silver (cleaned), gold (business-ready)."
    ),
    schema={"sql": str, "database_path": str},
)
async def query_warehouse(args: dict) -> dict:
    import duckdb

    sql: str = args["sql"]
    db_path: str = args.get("database_path", DEFAULT_DB_PATH)

    try:
        conn = duckdb.connect(db_path, read_only=True)
        result = conn.execute(sql).fetchdf()
        conn.close()

        row_count = len(result)
        if row_count == 0:
            return {"content": [{"type": "text", "text": "Query returned 0 rows."}]}

        if row_count > 100:
            text = (
                f"Query returned {row_count} rows. Showing first 50:\n\n"
                f"{result.head(50).to_markdown(index=False)}\n\n"
                f"... and {row_count - 50} more rows."
            )
        else:
            text = result.to_markdown(index=False)

        return {"content": [{"type": "text", "text": text}]}

    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Query error: {e}"}],
            "isError": True,
        }


@tool(
    name="list_tables",
    description=(
        "List all tables and views in the ecommerce DuckDB warehouse, "
        "organized by schema. Shows table type and estimated row count."
    ),
    schema={"database_path": str},
)
async def list_tables(args: dict) -> dict:
    import duckdb

    db_path: str = args.get("database_path", DEFAULT_DB_PATH)

    try:
        conn = duckdb.connect(db_path, read_only=True)
        result = conn.execute("""
            SELECT
                table_schema AS schema_name,
                table_name,
                table_type,
                estimated_size AS est_rows
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """).fetchdf()
        conn.close()

        if len(result) == 0:
            return {"content": [{"type": "text", "text": "No tables found in database."}]}

        return {"content": [{"type": "text", "text": result.to_markdown(index=False)}]}

    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {e}"}],
            "isError": True,
        }


@tool(
    name="describe_table",
    description="Show columns, types, and nullable status for a specific table.",
    schema={"table_name": str, "database_path": str},
)
async def describe_table(args: dict) -> dict:
    import duckdb

    table: str = args["table_name"]
    db_path: str = args.get("database_path", DEFAULT_DB_PATH)

    try:
        conn = duckdb.connect(db_path, read_only=True)
        result = conn.execute(f"DESCRIBE {table}").fetchdf()
        conn.close()

        return {"content": [{"type": "text", "text": result.to_markdown(index=False)}]}

    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {e}"}],
            "isError": True,
        }


@tool(
    name="data_quality_check",
    description=(
        "Run automated data quality checks on a table: null rates per column, "
        "duplicate primary key detection, row count, and freshness check on "
        "timestamp columns. Use to validate gold layer tables."
    ),
    schema={"table_name": str, "primary_key": str, "database_path": str},
)
async def data_quality_check(args: dict) -> dict:
    import duckdb

    table: str = args["table_name"]
    pk: str = args.get("primary_key", "id")
    db_path: str = args.get("database_path", DEFAULT_DB_PATH)

    try:
        conn = duckdb.connect(db_path, read_only=True)
        lines: list[str] = [f"## Data Quality Report: `{table}`\n"]

        # Row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        lines.append(f"**Total rows**: {row_count:,}")

        # Duplicate PK check
        try:
            dup_count = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk}, COUNT(*) as cnt
                    FROM {table}
                    GROUP BY {pk}
                    HAVING cnt > 1
                )
            """).fetchone()[0]

            if dup_count > 0:
                lines.append(f"🔴 **Duplicate {pk}s**: {dup_count:,} duplicates found!")
            else:
                lines.append(f"✅ **Primary key ({pk})**: No duplicates")
        except Exception:
            lines.append(f"⚠️ Could not check PK `{pk}` — column may not exist")

        # Null rates
        cols = conn.execute(f"DESCRIBE {table}").fetchdf()
        null_lines: list[str] = []
        for _, row in cols.iterrows():
            col_name = row["column_name"]
            null_pct = conn.execute(f"""
                SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE {col_name} IS NULL)
                       / NULLIF(COUNT(*), 0), 1)
                FROM {table}
            """).fetchone()[0]

            if null_pct and null_pct > 0:
                emoji = "🔴" if null_pct > 50 else ("🟡" if null_pct > 5 else "🟢")
                null_lines.append(f"  {emoji} `{col_name}`: {null_pct}% null")

        if null_lines:
            lines.append("\n**Null rates**:")
            lines.extend(null_lines)
        else:
            lines.append("✅ **No nulls** in any column")

        # Freshness (check timestamp-ish columns)
        ts_cols = cols[cols["column_type"].str.contains("TIMESTAMP|DATE", case=False, na=False)]
        for _, row in ts_cols.iterrows():
            col_name = row["column_name"]
            max_val = conn.execute(f"SELECT MAX({col_name}) FROM {table}").fetchone()[0]
            lines.append(f"**Freshness ({col_name})**: latest = `{max_val}`")

        conn.close()
        return {"content": [{"type": "text", "text": "\n".join(lines)}]}

    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"DQ check error: {e}"}],
            "isError": True,
        }


def create_warehouse_server():
    """Create the in-process MCP server with all ecommerce DuckDB tools."""
    return create_sdk_mcp_server(
        name="warehouse",
        version="1.0.0",
        tools=[query_warehouse, list_tables, describe_table, data_quality_check],
    )
