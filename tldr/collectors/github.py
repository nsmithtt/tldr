import requests
import yaml
import os
from datetime import datetime, timezone, timedelta

from tldr import db


CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "config.yaml")


def get_gh_token():
    token = os.environ.get("GH_TOKEN")
    if not token:
        raise RuntimeError("GH_TOKEN environment variable not set.")
    return token


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def gh_api(endpoint, token, params=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    url = f"https://api.github.com{endpoint}"
    items = []
    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        items.extend(resp.json())
        url = resp.links.get("next", {}).get("url")
        params = None  # params are baked into the next URL
    return items


def collect_commits(owner, repo, token, since):
    params = {"per_page": 100}
    if since:
        params["since"] = since
    commits = gh_api(f"/repos/{owner}/{repo}/commits", token, params)
    count = 0
    for c in commits:
        event = {
            "timestamp": c["commit"]["author"]["date"],
            "source": "github",
            "project": f"{owner}/{repo}",
            "event_type": "commit",
            "author": c["commit"]["author"]["name"],
            "title": c["commit"]["message"].split("\n")[0],
            "body": c["commit"]["message"],
            "url": c["html_url"],
            "raw": c,
            "source_id": c["sha"],
        }
        count += db.insert_event(event)
    return count


def collect_pull_requests(owner, repo, token, since):
    params = {"state": "all", "sort": "updated", "direction": "desc", "per_page": 100}
    if since:
        params["since"] = since
    prs = gh_api(f"/repos/{owner}/{repo}/pulls", token, params)
    count = 0
    for pr in prs:
        if since and pr["updated_at"] < since:
            continue
        if pr["merged_at"]:
            etype = "pr_merged"
            ts = pr["merged_at"]
        elif pr["state"] == "closed":
            continue  # closed without merge, skip
        else:
            etype = "pr_opened"
            ts = pr["created_at"]
        event = {
            "timestamp": ts,
            "source": "github",
            "project": f"{owner}/{repo}",
            "event_type": etype,
            "author": pr["user"]["login"],
            "title": pr["title"],
            "body": pr.get("body"),
            "url": pr["html_url"],
            "raw": pr,
            "source_id": f"pr-{pr['number']}",
        }
        count += db.insert_event(event)
    return count


def collect_reviews(owner, repo, token, since):
    params = {"state": "all", "sort": "updated", "direction": "desc", "per_page": 100}
    prs = gh_api(f"/repos/{owner}/{repo}/pulls", token, params)
    count = 0
    for pr in prs:
        if since and pr["updated_at"] < since:
            continue
        reviews = gh_api(f"/repos/{owner}/{repo}/pulls/{pr['number']}/reviews", token)
        for r in reviews:
            if since and r["submitted_at"] and r["submitted_at"] < since:
                continue
            event = {
                "timestamp": r["submitted_at"] or pr["updated_at"],
                "source": "github",
                "project": f"{owner}/{repo}",
                "event_type": "review",
                "author": r["user"]["login"],
                "title": f"Review on #{pr['number']}: {pr['title']} ({r['state']})",
                "body": r.get("body"),
                "url": r["html_url"],
                "raw": r,
                "source_id": f"review-{r['id']}",
            }
            count += db.insert_event(event)
    return count


def collect_issues(owner, repo, token, since):
    params = {"state": "all", "sort": "updated", "direction": "desc", "per_page": 100, "filter": "all"}
    if since:
        params["since"] = since
    issues = gh_api(f"/repos/{owner}/{repo}/issues", token, params)
    count = 0
    for issue in issues:
        if "pull_request" in issue:
            continue  # skip PRs (GitHub returns them as issues too)
        if issue["state"] == "closed":
            etype = "issue_closed"
            ts = issue["closed_at"] or issue["updated_at"]
        else:
            etype = "issue_opened"
            ts = issue["created_at"]
        event = {
            "timestamp": ts,
            "source": "github",
            "project": f"{owner}/{repo}",
            "event_type": etype,
            "author": issue["user"]["login"],
            "title": issue["title"],
            "body": issue.get("body"),
            "url": issue["html_url"],
            "raw": issue,
            "source_id": f"issue-{issue['number']}",
        }
        count += db.insert_event(event)
    return count


COLLECTORS = {
    "commits": collect_commits,
    "pull_requests": collect_pull_requests,
    "reviews": collect_reviews,
    "issues": collect_issues,
}


def collect():
    config = load_config()
    gh_config = config["github"]
    token = get_gh_token()
    db.init_db()

    total = 0
    for repo in gh_config["repos"]:
        owner, name = repo.split("/")
        since = db.get_high_water_mark("github", repo)
        if not since:
            # Default to 30 days back on first run
            since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        print(f"Collecting {repo} (since {since})")

        for event_type in gh_config.get("event_types", []):
            collector = COLLECTORS.get(event_type)
            if not collector:
                print(f"  Unknown event type: {event_type}")
                continue
            try:
                count = collector(owner, name, token, since)
                print(f"  {event_type}: {count} new events")
                total += count
            except requests.HTTPError as e:
                msg = e.response.json().get("message", "") if e.response is not None else ""
                print(f"  {event_type}: error - {e}")
                if msg:
                    print(f"    {msg}")

    print(f"\nTotal new events: {total}")
