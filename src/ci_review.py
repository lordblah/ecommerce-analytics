"""
CI Agent Review
=================
Runs medallion architecture validation on git changes.
Exits non-zero if critical violations found. Safe for GitHub Actions.

Usage:
    python src/ci_review.py

Environment:
    ANTHROPIC_API_KEY  — required
    CI_REVIEW_BUDGET   — max cost in USD (default: 1.00)
"""

from __future__ import annotations

import os
import sys
import anyio
from claude_agent_sdk import query, ClaudeAgentOptions


async def ci_review() -> int:
    """Run medallion review on changed files. Returns exit code."""

    budget = float(os.getenv("CI_REVIEW_BUDGET", "1.00"))

    options = ClaudeAgentOptions(
        agents={
            "medallion-reviewer": {
                "description": "Validates dbt models follow medallion architecture",
                "prompt": (
                    "Review ONLY the changed dbt models. Check:\n"
                    "1. Layer compliance (bronze/silver/gold boundaries)\n"
                    "2. ref() chains don't cross layers\n"
                    "3. Gold models have schema.yml with tests\n"
                    "4. Materializations match layer conventions\n\n"
                    "End your response with exactly one of:\n"
                    "VERDICT: PASS — no critical issues\n"
                    "VERDICT: FAIL — critical violations found"
                ),
                "tools": ["Read", "Grep", "Glob", "Bash"],
                "model": "sonnet",
            },
        },
        allowed_tools=["Read", "Grep", "Glob", "Bash", "Agent"],
        # Plan mode — agent can read and run safe commands but can't write
        permission_mode="plan",
        model="sonnet",
    )

    result_text = ""
    total_cost = 0.0

    print("🔍 CI Agent Review starting...\n")

    async for message in query(
        prompt=(
            "Run `git diff --name-only HEAD~1` to find changed files. "
            "If any .sql files in models/ changed, use the medallion-reviewer "
            "to validate them. If no model files changed, just PASS. "
            "End with VERDICT: PASS or VERDICT: FAIL."
        ),
        options=options,
    ):
        msg_type = type(message).__name__

        if msg_type == "TaskCompletedMessage":
            cost = getattr(message, "total_cost_usd", 0) or 0
            total_cost += cost
            if total_cost > budget:
                print(f"⚠️  Budget exceeded: ${total_cost:.2f} > ${budget:.2f}")
                return 1

        if hasattr(message, "result"):
            result_text = message.result

    print(result_text)
    print(f"\n💰 Total cost: ${total_cost:.4f}")

    if "VERDICT: FAIL" in result_text:
        print("\n❌ CI Review FAILED — critical medallion violations found")
        return 1

    print("\n✅ CI Review PASSED")
    return 0


if __name__ == "__main__":
    exit_code = anyio.run(ci_review)
    sys.exit(exit_code)
