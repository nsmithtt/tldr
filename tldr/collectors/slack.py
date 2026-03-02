import requests
import yaml
import os
from datetime import datetime, timezone, timedelta

from tldr import db


CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "config.yaml")


def get_slack_token():
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN environment variable not set.")
    return token


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def slack_api(method, token, params=None):
    """Call a Slack Web API method. Returns the JSON response body.

    For paginated endpoints, follows response_metadata.next_cursor and
    returns a flat list of items from the specified collection key.
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://slack.com/api/{method}"
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack API error ({method}): {data.get('error', data)}")
    return data


def slack_api_paginated(method, token, collection_key, params=None):
    """Call a paginated Slack API method, following next_cursor."""
    params = dict(params or {})
    items = []
    while True:
        data = slack_api(method, token, params)
        items.extend(data.get(collection_key, []))
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
        params["cursor"] = cursor
    return items


def resolve_channel_ids(channel_names, token):
    """Map channel names to IDs by listing conversations."""
    channels = slack_api_paginated(
        "conversations.list", token, "channels",
        params={"types": "public_channel", "limit": 200},
    )
    name_to_id = {c["name"]: c["id"] for c in channels}
    result = {}
    for name in channel_names:
        name = name.lstrip("#")
        if name in name_to_id:
            result[name] = name_to_id[name]
        else:
            print(f"  Warning: channel #{name} not found")
    return result


def message_url(team_domain, channel_id, ts):
    """Build a Slack deep-link for a message."""
    ts_flat = ts.replace(".", "")
    return f"https://slack.com/archives/{channel_id}/p{ts_flat}"


def ts_to_iso(ts):
    """Convert a Slack ts (Unix epoch string) to ISO 8601."""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def collect_channel(channel_name, channel_id, token, oldest):
    """Collect messages and thread replies for a single channel."""
    params = {"channel": channel_id, "limit": 200}
    if oldest:
        params["oldest"] = oldest

    messages = slack_api_paginated(
        "conversations.history", token, "messages", params,
    )

    count = 0
    for msg in messages:
        # Skip non-user messages (join/leave, bot integration, etc.)
        if msg.get("subtype"):
            continue

        ts = msg["ts"]
        text = msg.get("text", "")
        first_line = text.split("\n", 1)[0][:200]

        event = {
            "timestamp": ts_to_iso(ts),
            "source": "slack",
            "project": channel_name,
            "event_type": "message",
            "author": msg.get("user", "unknown"),
            "title": first_line,
            "body": text,
            "url": message_url("app", channel_id, ts),
            "raw": msg,
            "source_id": f"{channel_id}-{ts}",
        }
        count += db.insert_event(event)

        # Collect thread replies if any
        if msg.get("reply_count", 0) > 0:
            count += collect_thread(
                channel_name, channel_id, token, ts, first_line,
            )

    return count


def collect_thread(channel_name, channel_id, token, thread_ts, parent_title):
    """Collect replies in a thread (excluding the parent message)."""
    replies = slack_api_paginated(
        "conversations.replies", token, "messages",
        params={"channel": channel_id, "ts": thread_ts, "limit": 200},
    )

    count = 0
    for reply in replies:
        # The first message in replies is the parent; skip it
        if reply["ts"] == thread_ts:
            continue
        if reply.get("subtype"):
            continue

        ts = reply["ts"]
        text = reply.get("text", "")

        event = {
            "timestamp": ts_to_iso(ts),
            "source": "slack",
            "project": channel_name,
            "event_type": "thread_reply",
            "author": reply.get("user", "unknown"),
            "title": f"Reply in thread: {parent_title}",
            "body": text,
            "url": message_url("app", channel_id, ts),
            "raw": reply,
            "source_id": f"{channel_id}-{ts}",
        }
        count += db.insert_event(event)

    return count


def collect():
    config = load_config()
    slack_config = config.get("slack", {})
    channel_entries = slack_config.get("channels", [])
    if not channel_entries:
        print("No Slack channels configured in config.yaml")
        return

    token = get_slack_token()
    db.init_db()

    # Split entries into IDs (start with C/G) and names that need resolution
    channel_map = {}
    names_to_resolve = []
    for entry in channel_entries:
        entry = entry.lstrip("#")
        if entry[:1] in ("C", "G") and entry[1:].isalnum():
            # Looks like a channel ID — use it directly
            channel_map[entry] = entry
        else:
            names_to_resolve.append(entry)

    if names_to_resolve:
        print("Resolving channel IDs...")
        channel_map.update(resolve_channel_ids(names_to_resolve, token))

    if not channel_map:
        print("No matching channels found.")
        return

    total = 0
    for name, channel_id in channel_map.items():
        since = db.get_high_water_mark("slack", name)
        if since:
            # Convert ISO timestamp back to Unix epoch for Slack's oldest param
            oldest = str(datetime.fromisoformat(since).timestamp())
        else:
            oldest = str((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())
        print(f"Collecting #{name} (since {since or '30 days ago'})")

        try:
            count = collect_channel(name, channel_id, token, oldest)
            print(f"  {count} new events")
            total += count
        except Exception as e:
            print(f"  Error: {e}")

    print(f"\nTotal new events: {total}")
