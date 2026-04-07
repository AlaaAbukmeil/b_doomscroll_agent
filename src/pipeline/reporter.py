import json
from datetime import datetime, timezone


class Reporter:
    def __init__(self, db):
        self.db = db

    def print_report(self):
        stats = self.db.get_stats()
        insights = self.db.get_latest_insights(limit=3)

        print(f"\n{'='*60}")
        print(f"  DOOMSCROLL AGENT — STATUS REPORT")
        print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'='*60}\n")

        print("Database:")
        print(f"  Posts scraped:     {stats['total_posts']}")
        print(f"  Posts analyzed:    {stats['analyzed_posts']}")
        print(f"  Insights stored:   {stats['total_insights']}")
        print(f"  Subreddits:        {', '.join(stats['subreddits']) or 'none'}")

        if stats.get("sentiment_distribution"):
            print(f"\nOverall Sentiment:")
            for label, count in sorted(stats["sentiment_distribution"].items(),
                                       key=lambda x: x[1], reverse=True):
                bar = "█" * min(count, 40)
                print(f"  {label:12s} {bar} {count}")

        if not insights:
            print("\nNo insights yet. Run: python main.py --mode full\n")
            return

        for i, ins in enumerate(insights):
            print(f"\n{'─'*60}")
            print(f"Insight #{i+1}  |  {ins.get('generated_at', '')}  |  {ins.get('post_count', 0)} posts")

            try:
                topics = json.loads(ins.get("trending_topics", "[]"))
                if topics:
                    print(f"\n  Trending Topics:")
                    for topic, count in topics[:7]:
                        print(f"    • {topic} ({count})")
            except (json.JSONDecodeError, TypeError):
                pass

            if ins.get("actionable_insights"):
                print(f"\n  Actionable Insights:")
                # Print first 600 chars, indented
                text = ins["actionable_insights"][:600]
                for line in text.split("\n"):
                    print(f"    {line}")
                if len(ins["actionable_insights"]) > 600:
                    print(f"    ... (truncated)")

        print(f"\n{'='*60}\n")