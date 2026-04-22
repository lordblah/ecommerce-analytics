# CLAUDE.md — Ecommerce Analytics Agent Fleet

## What This Is
A multi-agent system built on the Claude Agent SDK, designed to review, debug,
and analyze the ecommerce-analytics pipeline (Dagster + dbt + DuckDB + Streamlit).
Uses medallion architecture: bronze (raw) → silver (cleaned) → gold (business-ready).

## Target Project
- Repo: github.com/lordblah/ecommerce-analytics
- Stack: Dagster orchestration → dbt transformations → DuckDB warehouse → Streamlit dashboards
- Platform: Windows (developed locally)
- Architecture: Medallion (bronze/silver/gold layers)
- DB: DuckDB (embedded OLAP, single-writer constraint)

## Agent Fleet
| Agent | Role | Model | Tools |
|-------|------|-------|-------|
| medallion-reviewer | Validates medallion layer compliance | sonnet | Read, Grep, Glob |
| dagster-debugger | Traces pipeline failures and asset issues | sonnet | Read, Grep, Glob, Bash |
| duckdb-analyst | Queries the warehouse, validates data quality | sonnet | MCP DuckDB tools |
| streamlit-reviewer | Reviews dashboard code and metric consistency | haiku | Read, Grep, Glob |
| code-reviewer | General Python quality, security, types | haiku | Read, Grep, Glob |

## Conventions
- Subagent names: kebab-case
- MCP tools: mcp__{server}__{tool} pattern
- All async via anyio
- Never put "Agent" in a subagent's allowed_tools
- Filesystem agents in .claude/agents/ need setting_sources=["project"] to load

## Medallion Layer Rules (enforced by medallion-reviewer)
- **Bronze**: Raw ingestion only. source() references. No business logic. Views or tables.
- **Silver**: Cleaned/typed/deduped. ref() to bronze only. Tests on PKs. Mostly views.
- **Gold**: Business-ready aggregates/metrics. ref() to silver only. Tables or incremental. Full docs + tests.
- Cross-layer refs (gold→bronze, silver→gold) are violations.
