import time
import json
import feedparser
from datetime import datetime, timezone

SUBREDDITS = ["MalayalamMovies","movies", "boxoffice", "television"]

def fetch_reddit_rss_stream(poll_interval_sec=5):
    """
    Fetches live Reddit discussions via open RSS feeds without hitting 403 blocks.
    """
    seen_entry_ids = set()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Reddit RSS Stream Collector...")
    print(f"Target subreddits: {', '.join(SUBREDDITS)} (Polling every {poll_interval_sec}s)\n")

    try:
        while True:
            for sub in SUBREDDITS:
                feed_url = f"https://www.reddit.com/r/{sub}/new/.rss"
                feed = feedparser.parse(
                    feed_url,
                    agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )

                for entry in reversed(feed.entries):
                    post_id = entry.get("id", "")
                    if post_id and post_id not in seen_entry_ids:
                        seen_entry_ids.add(post_id)

                        # Bounded memory cache
                        if len(seen_entry_ids) > 1000:
                            seen_entry_ids.pop()

                        payload = {
                            "comment_id": entry.get("id", "").split("/")[-2] if "/" in entry.get("id", "") else entry.get("id"),
                            "post_id": entry.get("id", "").split("/")[-2] if "/" in entry.get("id", "") else entry.get("id"),
                            "post_title": entry.get("title", ""),
                            "subreddit": sub,
                            "author": entry.get("author", "reddit_user"),
                            "body": entry.get("summary", "").replace("\n", " ").strip(),
                            "score": 1,
                            "created_utc": entry.get("updated", datetime.now(timezone.utc).isoformat()),
                            "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
                        }

                        print(json.dumps(payload, indent=2))
                        print("-" * 60)

            time.sleep(poll_interval_sec)

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")

if __name__ == "__main__":
    fetch_reddit_rss_stream(poll_interval_sec=10)