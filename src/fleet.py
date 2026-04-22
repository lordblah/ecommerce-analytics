"""
Ecommerce Analytics Agent Fleet
=================================
The main multi-agent orchestrator for the ecommerce-analytics project.
Spawns specialized subagents for medallion architecture review, Dagster
debugging, DuckDB analysis, Streamlit review, and general code quality.

Usage:
    # Full review (all agents available)
    python src/fleet.py

    # Custom prompt — parent decides which agents to invoke
    python src/fleet.py "Check if my gold models have proper test coverage"

    # Force a specific agent
    python src/fleet.py "Use the dagster-debugger agent to check why my assets fail"
"""

from __future__ import annotations

import sys
import time
import anyio
from claude_agent_sdk import query, ClaudeAgentOptions
from tools.duckdb_tools import create_warehouse_server


# ---------------------------------------------------------------------------
# Agent definitions (inline — these override .claude/agents/ if same name)
# ---------------------------------------------------------------------------

AGENTS: dict[str, dict] = {
    "medallion-reviewer": {
        "description": (
            "Validates dbt models follow medallion architecture (bronze → silver → gold). "
            "Checks ref() chains, layer compliance, materialization strategy, naming "
            "conventions, and test coverage per layer. Catches cross-layer violations."
        ),
        "prompt": (
            "You are a senior analytics engineer enforcing medallion architecture in a "
            "dbt + DuckDB project.\n\n"
            "RULES:\n"
            "- Bronze: source() only, no joins/aggs, raw data. views.\n"
            "- Silver: ref() to bronze only. cleaned/typed. unique+not_null on PKs.\n"
            "- Gold: ref() to silver only. business metrics. tables/incremental. full docs+tests.\n"
            "- Cross-layer refs (gold→bronze) are CRITICAL violations.\n\n"
            "PROCESS:\n"
            "1. Glob all .sql files in models/\n"
            "2. Classify each by layer (path + naming)\n"
            "3. Grep for ref() and source() to map dependencies\n"
            "4. Check schema.yml for tests and docs\n"
            "5. Read dbt_project.yml for materializations\n\n"
            "Output: structured report with 🔴 critical / 🟡 warning / 🟢 suggestion."
        ),
        "tools": ["Read", "Grep", "Glob"],
        "model": "sonnet",
    },
    "dagster-debugger": {
        "description": (
            "Debugs Dagster pipeline issues — asset failures, dbt integration errors, "
            "DuckDB IO manager problems, resource configs, schedule issues."
        ),
        "prompt": (
            "You debug a Dagster + dbt + DuckDB ecommerce pipeline on Windows.\n"
            "Key areas:\n"
            "- @dbt_assets config and manifest path\n"
            "- DuckDB single-writer lock (concurrent materializations fail)\n"
            "- Windows path issues (forward vs backslash)\n"
            "- Resource bindings in Definitions()\n"
            "- dbt profiles.yml → DuckDB path alignment\n"
            "Trace symptom → root cause. Provide the specific fix."
        ),
        "tools": ["Read", "Grep", "Glob", "Bash"],
        "model": "sonnet",
    },
    "duckdb-analyst": {
        "description": (
            "Queries the DuckDB ecommerce warehouse for data analysis. "
            "Validates metrics, checks data quality, explores schemas, "
            "and verifies gold layer table correctness."
        ),
        "prompt": (
            "You are a data analyst with access to an ecommerce DuckDB warehouse "
            "using medallion architecture (bronze/silver/gold).\n\n"
            "WORKFLOW:\n"
            "1. List tables to understand what's available\n"
            "2. Describe tables before querying\n"
            "3. Run data_quality_check on gold tables\n"
            "4. Write efficient analytical queries\n\n"
            "Summarize with specific numbers and business insights."
        ),
        "tools": [
            "mcp__warehouse__query_warehouse",
            "mcp__warehouse__list_tables",
            "mcp__warehouse__describe_table",
            "mcp__warehouse__data_quality_check",
        ],
        "model": "sonnet",
    },
    "streamlit-reviewer": {
        "description": (
            "Reviews Streamlit dashboard code for the ecommerce project. "
            "Checks metric consistency with gold models, DuckDB connection patterns, "
            "caching, and UI quality."
        ),
        "prompt": (
            "You review Streamlit dashboards on a DuckDB + dbt stack.\n"
            "Check:\n"
            "- Dashboards query gold tables only (not bronze/silver)\n"
            "- DuckDB connections use read_only=True\n"
            "- @st.cache_data on expensive queries\n"
            "- No hardcoded SQL that duplicates dbt logic\n"
            "- Proper error handling for locked/missing DB files\n"
            "Keep feedback concise."
        ),
        "tools": ["Read", "Grep", "Glob"],
        "model": "haiku",
    },
    "code-reviewer": {
        "description": (
            "General Python code review — types, security, error handling, "
            "Windows compatibility. Not for dbt SQL or Dagster-specific issues."
        ),
        "prompt": (
            "Python code reviewer. Focus on type hints, error handling, security "
            "(hardcoded secrets, SQL injection), Windows path issues, unused imports. "
            "Prioritize bugs over style."
        ),
        "tools": ["Read", "Grep", "Glob"],
        "model": "haiku",
    },
}


async def run_fleet(prompt: str, db_path: str | None = None) -> None:
    """Run the full ecommerce agent fleet."""

    # Setup MCP server for DuckDB access
    warehouse_server = create_warehouse_server()

    options = ClaudeAgentOptions(
        agents=AGENTS,
        mcp_servers={"warehouse": warehouse_server},
        allowed_tools=[
            # Parent orchestrator tools
            "Read", "Write", "Bash", "Grep", "Glob",
            # Enable subagent delegation
            "Agent",
            # Parent can also query DuckDB directly
            "mcp__warehouse__query_warehouse",
            "mcp__warehouse__list_tables",
        ],
        model="sonnet",
    )

    # Tracking
    start_time = time.time()
    subagent_count = 0

    print(f"🚀 Fleet dispatching: {prompt[:100]}...")
    print(f"   Agents available: {', '.join(AGENTS.keys())}")
    print()

    async for message in query(prompt=prompt, options=options):
        msg_type = type(message).__name__

        if msg_type == "TaskStartedMessage":
            subagent_count += 1
            desc = getattr(message, "description", "unknown")
            print(f"  🤖 [{subagent_count}] Subagent spawned: {desc}")

        elif msg_type == "TaskCompletedMessage":
            cost = getattr(message, "total_cost_usd", None)
            duration = getattr(message, "duration_ms", None)
            cost_str = f"${cost:.4f}" if cost else "n/a"
            dur_str = f"{duration / 1000:.1f}s" if duration else "n/a"
            print(f"  ✅ Subagent completed ({cost_str}, {dur_str})")

        elif hasattr(message, "result"):
            elapsed = time.time() - start_time
            print("\n" + "=" * 70)
            print(f"FLEET RESULT ({subagent_count} subagents, {elapsed:.1f}s)")
            print("=" * 70)
            print(message.result)


# ---------------------------------------------------------------------------
# Default prompts for common workflows
# ---------------------------------------------------------------------------

PROMPTS = {
    "full-review": (
        "Do a comprehensive review of this ecommerce analytics project:\n"
        "1. Use the medallion-reviewer to audit the dbt model layering\n"
        "2. Use the dagster-debugger to check the pipeline configuration\n"
        "3. Use the streamlit-reviewer to check the dashboard code\n"
        "4. Use the code-reviewer for general Python quality\n"
        "Give me a unified report with prioritized findings."
    ),
    "medallion-audit": (
        "Use the medallion-reviewer agent to do a thorough audit of the dbt models. "
        "Check every model for layer compliance, ref() chain correctness, "
        "materialization strategy, and test coverage."
    ),
    "data-quality": (
        "Use the duckdb-analyst agent to:\n"
        "1. List all tables in the warehouse\n"
        "2. Run data quality checks on every gold-layer table\n"
        "3. Summarize any data quality issues found"
    ),
    "pipeline-check": (
        "Use the dagster-debugger agent to review the entire pipeline config. "
        "Check asset definitions, resource bindings, DuckDB IO manager setup, "
        "and flag any issues that would cause failures."
    ),
}


async def main() -> None:
    if len(sys.argv) < 2:
        prompt = PROMPTS["full-review"]
        print("No prompt given — running full review.\n")
        print("Available presets: full-review, medallion-audit, data-quality, pipeline-check")
        print("Usage: python src/fleet.py \"your prompt here\"")
        print(f"       python src/fleet.py --preset medallion-audit\n")
    elif sys.argv[1] == "--preset" and len(sys.argv) > 2:
        preset = sys.argv[2]
        if preset not in PROMPTS:
            print(f"Unknown preset: {preset}")
            print(f"Available: {', '.join(PROMPTS.keys())}")
            return
        prompt = PROMPTS[preset]
    else:
        prompt = " ".join(sys.argv[1:])

    await run_fleet(prompt)


if __name__ == "__main__":
    anyio.run(main)
