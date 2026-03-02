import argparse
import sys

from dotenv import load_dotenv
load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="tldr — Weekly Activity Digest")
    subparsers = parser.add_subparsers(dest="command")

    # collect
    collect_parser = subparsers.add_parser("collect", help="Collect events from a source")
    collect_parser.add_argument("source", nargs="?", choices=["github", "slack"], help="Source to collect from (omit to collect all)")

    # summarize
    summarize_parser = subparsers.add_parser("summarize", help="Generate a weekly digest")
    summarize_parser.add_argument("--days", type=int, default=7, help="Number of days to look back")

    args = parser.parse_args()

    if args.command == "collect":
        sources = [args.source] if args.source else ["github", "slack"]
        for source in sources:
            if source == "github":
                from tldr.collectors.github import collect as collect_github
                collect_github()
            elif source == "slack":
                from tldr.collectors.slack import collect as collect_slack
                collect_slack()
    elif args.command == "summarize":
        from tldr.summarize import summarize
        summarize(days=args.days)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
