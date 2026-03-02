import sqlite3
import json
import os
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tldr.db")


def get_connection(db_path=None):
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path=None):
    conn = get_connection(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            collected_at TEXT NOT NULL,
            source TEXT NOT NULL,
            project TEXT NOT NULL,
            event_type TEXT NOT NULL,
            author TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT,
            url TEXT,
            raw TEXT,
            source_id TEXT NOT NULL,
            UNIQUE(source, source_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_source_project
        ON events(source, project)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_timestamp
        ON events(timestamp)
    """)
    conn.commit()
    conn.close()


def insert_event(event, db_path=None):
    conn = get_connection(db_path)
    try:
        conn.execute("""
            INSERT OR IGNORE INTO events
            (timestamp, collected_at, source, project, event_type, author, title, body, url, raw, source_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["timestamp"],
            datetime.now(timezone.utc).isoformat(),
            event["source"],
            event["project"],
            event["event_type"],
            event["author"],
            event["title"],
            event.get("body"),
            event.get("url"),
            json.dumps(event.get("raw")) if event.get("raw") else None,
            event["source_id"],
        ))
        conn.commit()
        return conn.total_changes
    finally:
        conn.close()


def get_events(since, until=None, source=None, db_path=None):
    conn = get_connection(db_path)
    query = "SELECT * FROM events WHERE timestamp >= ?"
    params = [since]
    if until:
        query += " AND timestamp <= ?"
        params.append(until)
    if source:
        query += " AND source = ?"
        params.append(source)
    query += " ORDER BY timestamp"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_high_water_mark(source, project, db_path=None):
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT MAX(timestamp) as hw FROM events WHERE source = ? AND project = ?",
        (source, project),
    ).fetchone()
    conn.close()
    return row["hw"] if row and row["hw"] else None
