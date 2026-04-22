"""
Interactive Session — Iterative Ecommerce Analysis
=====================================================
Multi-turn session where each query builds on the previous.
The agent remembers what it read and analyzed across turns.

Workflow:
  Turn 1: Map the project and understand the medallion architecture
  Turn 2: Identify the biggest quality/architecture gap
  Turn 3: Generate the fix

Usage:
    cd /path/to/ecommerce-analytics
    python /path/to/ecommerce-agent-fleet/src/session_demo.py
"""

from __future__ import annotations

import anyio
from claude_agent_sdk import query, ClaudeAgentOptions


async def interactive_session() -> None:
    session_id: str | None = None

    # -------------------------------------------------------------------
    # Turn 1: Explore
    # -------------------------------------------------------------------
    print("=" * 60)
    print("TURN 1: Mapping the project")
    print("=" * 60)

    async for message in query(
        prompt=(
            "Read through this ecommerce analytics project. Map out:\n"
            "1. The dbt model DAG — which models exist in each medallion layer\n"
            "2. The Dagster asset definitions — how orchestration is configured\n"
            "3. The Streamlit dashboards — what metrics they display\n"
            "4. The DuckDB connection pattern — how each component connects\n"
            "Give me a clear architecture summary."
        ),
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Glob", "Grep"],
            model="sonnet",
        ),
    ):
        if hasattr(message, "session_id") and message.session_id:
            session_id = message.session_id
        if hasattr(message, "result"):
            print(message.result)

    if not session_id:
        print("⚠️  No session_id — can't resume.")
        return

    print(f"\n📎 Session: {session_id}\n")

    # -------------------------------------------------------------------
    # Turn 2: Diagnose
    # -------------------------------------------------------------------
    print("=" * 60)
    print("TURN 2: Finding the biggest gap")
    print("=" * 60)

    async for message in query(
        prompt=(
            "Based on what you just read, what's the single most impactful "
            "improvement I could make to this project? Consider:\n"
            "- Medallion layer violations\n"
            "- Missing tests or documentation\n"
            "- Dagster config issues that would cause failures\n"
            "- Streamlit performance or correctness problems\n"
            "Pick the ONE thing with the highest severity and explain why."
        ),
        options=ClaudeAgentOptions(
            session_id=session_id,
            allowed_tools=["Read", "Glob", "Grep"],
            model="sonnet",
        ),
    ):
        if hasattr(message, "result"):
            print(message.result)

    # -------------------------------------------------------------------
    # Turn 3: Fix
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("TURN 3: Generating the fix")
    print("=" * 60)

    async for message in query(
        prompt=(
            "Now generate the exact fix for the issue you identified. "
            "Show me the complete file contents I need to create or modify. "
            "If it's a dbt model, include the SQL and the schema.yml entry. "
            "If it's a Dagster config, show the corrected Python. "
            "Make it copy-paste ready."
        ),
        options=ClaudeAgentOptions(
            session_id=session_id,
            allowed_tools=["Read", "Write", "Glob", "Grep", "Bash"],
            model="sonnet",
        ),
    ):
        if hasattr(message, "result"):
            print(message.result)


if __name__ == "__main__":
    anyio.run(interactive_session)
