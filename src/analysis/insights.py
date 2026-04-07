import json
from src.dify_client import DifyClient


class InsightGenerator:
    def __init__(self, config, db):
        self.db = db
        self.dify = DifyClient(
            base_url=config["dify"]["base_url"],
            api_key=config["dify"]["insights_api_key"],
        )

    def generate(self):
        posts = self.db.get_analyzed_posts(limit=150)
        if not posts:
            return None

        agg = self._aggregate(posts)

        raw = self.dify.run_workflow(inputs={
            "aggregated_data": json.dumps(agg, indent=2),
            "post_count": str(len(posts)),
        })

        report_text = raw if isinstance(raw, str) else json.dumps(raw)

        insight = {
            "time_window": "latest",
            "post_count": len(posts),
            "trending_topics": agg["top_topics"],
            "sentiment_summary": agg["sentiment_distribution"],
            "actionable_insights": report_text,
            "raw_report": report_text,
        }

        self.db.store_insight(insight)
        return insight

    # ── Aggregation ─────────────────────────────────────────

    def _aggregate(self, posts):
        sentiments = {}
        topic_counts = {}
        keyword_counts = {}
        sub_counts = {}

        for p in posts:
            # Sentiment
            s = p.get("sentiment", "neutral")
            sentiments[s] = sentiments.get(s, 0) + 1

            # Topics
            for t in self._parse_json_list(p.get("topics", "[]")):
                t_lower = t.strip().lower()
                if t_lower:
                    topic_counts[t_lower] = topic_counts.get(t_lower, 0) + 1

            # Keywords
            for k in self._parse_json_list(p.get("keywords", "[]")):
                k_lower = k.strip().lower()
                if k_lower:
                    keyword_counts[k_lower] = keyword_counts.get(k_lower, 0) + 1

            # Subreddits
            sub = p.get("subreddit", "unknown")
            sub_counts[sub] = sub_counts.get(sub, 0) + 1

        top_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        top_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:15]

        # High-engagement posts for context
        top_posts = sorted(posts, key=lambda x: x.get("score", 0), reverse=True)[:5]
        top_post_summaries = [
            {
                "title": p["title"],
                "subreddit": p["subreddit"],
                "score": p["score"],
                "sentiment": p.get("sentiment", ""),
                "summary": p.get("summary", "")[:200],
            }
            for p in top_posts
        ]

        return {
            "sentiment_distribution": sentiments,
            "top_topics": top_topics,
            "top_keywords": top_keywords,
            "subreddit_breakdown": sub_counts,
            "high_engagement_posts": top_post_summaries,
            "total_posts": len(posts),
        }

    @staticmethod
    def _parse_json_list(val):
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                return parsed if isinstance(parsed, list) else []
            except (json.JSONDecodeError, TypeError):
                return []
        return []