import requests
import time
from datetime import datetime, timezone


class RedditScraper:
    BASE_URL = "https://www.reddit.com"

    def __init__(self, config):
        reddit_cfg = config.get("reddit", {})
        self.subreddits = reddit_cfg.get("subreddits", ["technology"])
        self.sort = reddit_cfg.get("sort", "hot")
        self.limit = reddit_cfg.get("posts_per_subreddit", 25)
        self.headers = {"User-Agent": "DoomscrollAgent/1.0 (research prototype)"}

    def scrape_subreddit(self, subreddit):
        url = f"{self.BASE_URL}/r/{subreddit}/{self.sort}.json"
        params = {"limit": self.limit, "raw_json": 1}

        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Warning: Failed to scrape r/{subreddit}: {e}")
            return []

        posts = []
        for child in data.get("data", {}).get("children", []):
            p = child["data"]

            # Skip pinned/stickied posts
            if p.get("stickied"):
                continue

            posts.append({
                "post_id": p["id"],
                "subreddit": subreddit,
                "title": p.get("title", ""),
                "selftext": (p.get("selftext") or "")[:2000],
                "author": p.get("author", "[deleted]"),
                "score": p.get("score", 0),
                "num_comments": p.get("num_comments", 0),
                "url": p.get("url", ""),
                "permalink": f"https://reddit.com{p.get('permalink', '')}",
                "created_utc": datetime.fromtimestamp(
                    p.get("created_utc", 0), tz=timezone.utc
                ).isoformat(),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "flair": p.get("link_flair_text") or "",
            })

        return posts

    def scrape_all(self):
        all_posts = []
        for sub in self.subreddits:
            print(f"  Scraping r/{sub}...")
            posts = self.scrape_subreddit(sub)
            all_posts.extend(posts)
            time.sleep(1.5)  # Reddit rate limit: stay under 1 req/sec
        return all_posts