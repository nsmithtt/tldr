import yaml
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import anthropic

from tldr import db


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.yaml")


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def format_events_for_prompt(events):
    by_project = defaultdict(lambda: defaultdict(list))
    for e in events:
        by_project[e["project"]][e["event_type"]].append(e)

    lines = []
    for project, types in sorted(by_project.items()):
        lines.append(f"## {project}")
        for etype, items in sorted(types.items()):
            lines.append(f"\n### {etype} ({len(items)})")
            for item in items:
                line = f"- [{item['timestamp'][:10]}] {item['author']}: {item['title']}"
                if item.get("url"):
                    line += f"  ({item['url']})"
                lines.append(line)
        lines.append("")
    return "\n".join(lines)


def summarize(post=False, days=None):
    config = load_config()
    summarize_config = config.get("summarize", {})
    model = summarize_config.get("model", "claude-sonnet-4-20250514")
    lookback = days or summarize_config.get("lookback_days", 7)

    db.init_db()
    since = (datetime.now(timezone.utc) - timedelta(days=lookback)).isoformat()
    events = db.get_events(since)

    if not events:
        print("No events found in the last {} days.".format(lookback))
        return

    formatted = format_events_for_prompt(events)
    print(f"Summarizing {len(events)} events from the last {lookback} days...\n")

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("Error: ANTHROPIC_API_KEY not set. Add it to .env or export it.")
        return

    client = anthropic.Anthropic()
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        system="""
You are a concise technical writer. Summarize the following week of development activity.
Highlight key changes, decisions, and patterns. Group by project.

Additionally create subsections for each project:
- Git commits and slack activity as development that has completed.
- PRs as upcoming changes to be aware of.
- Top issues as blockers or areas to raise attention.

Format the output using Slack mrkdwn syntax:
- Use *bold* and _italic_ for top level header including the date range.
- Use `code` for project section headers (not markdown ## or **).
- Use *bold* for subsection headers (not markdown ## or **).
- Use bullet points with \u2022 for list items.
- Use _italic_ for emphasis.
- Use `code` for inline code references.
- Use <url|link text> for links.
- Separate sections with a blank line.
- Do not use markdown headers, tables, or numbered lists.
        """,
        messages=[
            {"role": "user", "content": f"Here is the development activity:\n\n{formatted}"}
        ],
    )

    digest = response.content[0].text
    print(digest)

    slack_channel = summarize_config.get("slack_channel")
    if slack_channel and post:
        from tldr.collectors.slack import post_message, get_slack_token
        try:
            token = get_slack_token()
            post_message(slack_channel, digest, token)
            print(f"\nPosted digest to Slack channel {slack_channel}")
        except Exception as e:
            print(f"\nFailed to post to Slack: {e}")
