"""
Single Agent — Hello World
============================
Confirm the SDK works. One agent, read-only tools, map the project.

Usage:
    cd /path/to/ecommerce-analytics
    python /path/to/ecommerce-agent-fleet/src/single_agent.py
"""

import anyio
from claude_agent_sdk import query, ClaudeAgentOptions


async def main() -> None:
    print("🚀 Single agent scanning project...\n")

    async for message in query(
        prompt=(
            "Map this project's structure. Find all dbt models, Dagster assets, "
            "and Streamlit files. Tell me how the medallion architecture is organized "
            "and which layers exist (bronze/silver/gold or staging/intermediate/marts). "
            "Be specific about file paths."
        ),
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Glob", "Grep"],
            model="haiku",
        ),
    ):
        if hasattr(message, "result"):
            print(message.result)


if __name__ == "__main__":
    anyio.run(main)
