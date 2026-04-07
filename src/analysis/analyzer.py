import json
from src.dify_client import DifyClient


class PostAnalyzer:
    def __init__(self, config, db):
        self.db = db
        self.dify = DifyClient(
            base_url=config["dify"]["base_url"],
            api_key=config["dify"]["analyzer_api_key"],
        )
        self.batch_size = config.get("analysis", {}).get("batch_size", 20)

    def analyze_pending(self):
        posts = self.db.get_unanalyzed_posts(limit=self.batch_size)
        if not posts:
            print("  No unanalyzed posts found.")
            return 0

        count = 0
        for post in posts:
            try:
                result = self._analyze_single(post)
                self.db.store_analysis(
                    post_id=post["post_id"],
                    sentiment=result.get("sentiment", "neutral"),
                    sentiment_score=float(result.get("sentiment_score", 0.0)),
                    topics=result.get("topics", []),
                    keywords=result.get("keywords", []),
                    summary=result.get("summary", ""),
                )
                count += 1
            except Exception as e:
                print(f"  Warning: Failed to analyze {post['post_id']}: {e}")

        return count

    def _analyze_single(self, post):
        content = f"Title: {post['title']}\n"
        if post.get("selftext"):
            content += f"Body: {post['selftext'][:1000]}\n"
        content += (
            f"Subreddit: r/{post['subreddit']} | "
            f"Score: {post['score']} | "
            f"Comments: {post['num_comments']}"
        )
        if post.get("flair"):
            content += f" | Flair: {post['flair']}"

        raw = self.dify.run_workflow(inputs={"post_content": content})

        # Parse — Dify workflow should return JSON, but handle strings too
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                # Try parsing as JSON (may be wrapped in markdown code fence)
                cleaned = raw.strip()
                if cleaned.startswith("```"):
                    cleaned = "\n".join(cleaned.split("\n")[1:-1])
                return json.loads(cleaned)
            except json.JSONDecodeError:
                return {
                    "sentiment": "neutral",
                    "sentiment_score": 0.0,
                    "topics": [],
                    "keywords": [],
                    "summary": raw[:300],
                }
        return {"sentiment": "neutral", "sentiment_score": 0.0,
                "topics": [], "keywords": [], "summary": ""}