import argparse
import sys

from dotenv import load_dotenv
load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="tldr — Weekly Activity Digest")
    subparsers = parser.add_subparsers(dest="command")

    # collect
    collect_parser = subparsers.add_parser("collect", help="Collect events from a source")
    collect_parser.add_argument("source", choices=["github"], help="Source to collect from")

    # summarize
    summarize_parser = subparsers.add_parser("summarize", help="Generate a weekly digest")
    summarize_parser.add_argument("--days", type=int, default=7, help="Number of days to look back")

    args = parser.parse_args()

    if args.command == "collect":
        if args.source == "github":
            from tldr.collectors.github import collect
            collect()
    elif args.command == "summarize":
        from tldr.summarize import summarize
        summarize(days=args.days)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
