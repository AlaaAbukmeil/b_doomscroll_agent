import argparse
import sys
import time
import yaml
from pathlib import Path

from src.storage.database import Database
from src.scraper.reddit import RedditScraper
from src.analysis.analyzer import PostAnalyzer
from src.analysis.insights import InsightGenerator
from src.pipeline.reporter import Reporter


def load_config():
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("Error: config.yaml not found. Copy config.example.yaml and fill in your keys.")
        sys.exit(1)
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_scrape(scraper, db):
    posts = scraper.scrape_all()
    stored = db.store_posts(posts)
    print(f"Scraped {len(posts)} posts, stored {stored} new.")
    return stored


def run_analyze(analyzer):
    count = analyzer.analyze_pending()
    print(f"Analyzed {count} posts.")
    return count


def run_insights(insight_gen):
    result = insight_gen.generate()
    if result:
        print(f"\n{'='*60}")
        print(result.get("actionable_insights", "No insights generated."))
        print(f"{'='*60}\n")
    else:
        print("No analyzed posts available yet.")
    return result


def monitor(config, db, scraper, analyzer, insight_gen):
    interval = config.get("monitoring", {}).get("interval_minutes", 30)
    print(f"Starting continuous monitor (every {interval} min). Ctrl+C to stop.\n")

    try:
        cycle = 0
        while True:
            cycle += 1
            ts = time.strftime("%H:%M:%S")

            print(f"[{ts}] === Cycle {cycle} ===")

            print(f"[{ts}] Scraping...")
            posts = scraper.scrape_all()
            stored = db.store_posts(posts)
            print(f"  Fetched {len(posts)} posts, {stored} new.")

            print(f"[{ts}] Analyzing...")
            count = analyzer.analyze_pending()
            print(f"  Analyzed {count} posts.")

            if count > 0:
                print(f"[{ts}] Generating insights...")
                result = insight_gen.generate()
                if result:
                    print(f"  Insight stored. Topics: {result.get('trending_topics', [])[:3]}")

            print(f"[{ts}] Sleeping {interval} min...\n")
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")


def menu(config):
    db = Database()
    scraper = RedditScraper(config)
    analyzer = PostAnalyzer(config, db)
    insight_gen = InsightGenerator(config, db)
    reporter = Reporter(db)

    while True:
        print("\n=== G2: Social Knowledge Doomscroll Agent ===")
        print("1. Scrape    — Fetch latest posts from Reddit")
        print("2. Analyze   — Run sentiment & topic analysis")
        print("3. Insights  — Generate aggregate insights")
        print("4. Monitor   — Continuous loop (scrape → analyze → insights)")
        print("5. Report    — View latest trends and insights")
        print("6. Full Run  — Scrape + Analyze + Insights (one-shot)")
        print("7. Exit")

        choice = input("\nSelect [1-7]: ").strip()

        if choice == "1":
            run_scrape(scraper, db)
        elif choice == "2":
            run_analyze(analyzer)
        elif choice == "3":
            run_insights(insight_gen)
        elif choice == "4":
            monitor(config, db, scraper, analyzer, insight_gen)
        elif choice == "5":
            reporter.print_report()
        elif choice == "6":
            run_scrape(scraper, db)
            run_analyze(analyzer)
            run_insights(insight_gen)
        elif choice == "7":
            sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="G2: Doomscroll Agent")
    parser.add_argument("--mode", choices=["scrape", "analyze", "insights", "monitor", "report", "full"])
    parser.add_argument("--interval", type=int, help="Monitor interval in minutes")
    args = parser.parse_args()

    config = load_config()

    if args.interval:
        config.setdefault("monitoring", {})["interval_minutes"] = args.interval

    if args.mode:
        db = Database()
        scraper = RedditScraper(config)
        analyzer = PostAnalyzer(config, db)
        insight_gen = InsightGenerator(config, db)
        reporter = Reporter(db)

        if args.mode == "scrape":
            run_scrape(scraper, db)
        elif args.mode == "analyze":
            run_analyze(analyzer)
        elif args.mode == "insights":
            run_insights(insight_gen)
        elif args.mode == "monitor":
            monitor(config, db, scraper, analyzer, insight_gen)
        elif args.mode == "report":
            reporter.print_report()
        elif args.mode == "full":
            run_scrape(scraper, db)
            run_analyze(analyzer)
            run_insights(insight_gen)
    else:
        menu(config)


if __name__ == "__main__":
    main()