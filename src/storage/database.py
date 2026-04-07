import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone


class Database:
    def __init__(self, db_path="data/posts.db"):
        Path("data").mkdir(exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS posts (
                    post_id     TEXT PRIMARY KEY,
                    subreddit   TEXT NOT NULL,
                    title       TEXT,
                    selftext    TEXT,
                    author      TEXT,
                    score       INTEGER DEFAULT 0,
                    num_comments INTEGER DEFAULT 0,
                    url         TEXT,
                    permalink   TEXT,
                    created_utc TEXT,
                    scraped_at  TEXT,
                    flair       TEXT
                );

                CREATE TABLE IF NOT EXISTS analysis (
                    post_id         TEXT PRIMARY KEY,
                    sentiment       TEXT,
                    sentiment_score REAL,
                    topics          TEXT,
                    keywords        TEXT,
                    summary         TEXT,
                    analyzed_at     TEXT,
                    FOREIGN KEY (post_id) REFERENCES posts(post_id)
                );

                CREATE TABLE IF NOT EXISTS insights (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    generated_at        TEXT,
                    time_window         TEXT,
                    post_count          INTEGER,
                    trending_topics     TEXT,
                    sentiment_summary   TEXT,
                    actionable_insights TEXT,
                    raw_report          TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts(subreddit);
                CREATE INDEX IF NOT EXISTS idx_posts_created   ON posts(created_utc);
                CREATE INDEX IF NOT EXISTS idx_analysis_sent    ON analysis(sentiment);
            """)

    # ── Posts ────────────────────────────────────────────────

    def store_posts(self, posts):
        stored = 0
        with self._conn() as conn:
            for p in posts:
                cursor = conn.execute(
                    """INSERT OR IGNORE INTO posts
                       (post_id, subreddit, title, selftext, author, score,
                        num_comments, url, permalink, created_utc, scraped_at, flair)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (p["post_id"], p["subreddit"], p["title"], p["selftext"],
                     p["author"], p["score"], p["num_comments"], p["url"],
                     p["permalink"], p["created_utc"], p["scraped_at"], p["flair"]),
                )
                if cursor.rowcount > 0:
                    stored += 1
        return stored

    def get_unanalyzed_posts(self, limit=50):
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT p.* FROM posts p
                   LEFT JOIN analysis a ON p.post_id = a.post_id
                   WHERE a.post_id IS NULL
                   ORDER BY p.scraped_at DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Analysis ────────────────────────────────────────────

    def store_analysis(self, post_id, sentiment, sentiment_score, topics, keywords, summary):
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO analysis
                   (post_id, sentiment, sentiment_score, topics, keywords, summary, analyzed_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (post_id, sentiment, sentiment_score,
                 json.dumps(topics), json.dumps(keywords), summary,
                 datetime.now(timezone.utc).isoformat()),
            )

    def get_analyzed_posts(self, limit=200):
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT p.*, a.sentiment, a.sentiment_score,
                          a.topics, a.keywords, a.summary
                   FROM posts p
                   JOIN analysis a ON p.post_id = a.post_id
                   ORDER BY p.created_utc DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Insights ────────────────────────────────────────────

    def store_insight(self, insight):
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO insights
                   (generated_at, time_window, post_count, trending_topics,
                    sentiment_summary, actionable_insights, raw_report)
                   VALUES (?,?,?,?,?,?,?)""",
                (datetime.now(timezone.utc).isoformat(),
                 insight.get("time_window", ""),
                 insight.get("post_count", 0),
                 json.dumps(insight.get("trending_topics", [])),
                 json.dumps(insight.get("sentiment_summary", {})),
                 insight.get("actionable_insights", ""),
                 insight.get("raw_report", "")),
            )

    def get_latest_insights(self, limit=5):
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM insights ORDER BY generated_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Stats ───────────────────────────────────────────────

    def get_stats(self):
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
            analyzed = conn.execute("SELECT COUNT(*) FROM analysis").fetchone()[0]
            insight_count = conn.execute("SELECT COUNT(*) FROM insights").fetchone()[0]
            subs = [r[0] for r in conn.execute(
                "SELECT DISTINCT subreddit FROM posts"
            ).fetchall()]

            sent_dist = {}
            for row in conn.execute(
                "SELECT sentiment, COUNT(*) FROM analysis GROUP BY sentiment"
            ).fetchall():
                sent_dist[row[0]] = row[1]

            return {
                "total_posts": total,
                "analyzed_posts": analyzed,
                "total_insights": insight_count,
                "subreddits": subs,
                "sentiment_distribution": sent_dist,
            }