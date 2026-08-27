import time
import json
from datetime import datetime, timezone
import feedparser
from google.cloud import pubsub_v1

# GCP Configuration
PROJECT_ID = "cinepulse-analytics"
TOPIC_ID = "reddit-movie-stream"

SUBREDDITS = ["movies", "boxoffice", "television"]

def create_publisher():
    """Initializes the Pub/Sub Publisher Client."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    return publisher, topic_path

def start_publishing_stream(poll_interval_sec=8):
    publisher, topic_path = create_publisher()
    seen_entry_ids = set()

    print(f"[{datetime.now(timezone.utc).isoformat()}] Connected to GCP Pub/Sub Topic: {topic_path}")
    print(f"Streaming from r/{', r/'.join(SUBREDDITS)} ...\n")

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

                        # Pub/Sub requires bytes
                        data_bytes = json.dumps(payload).encode("utf-8")
                        
                        # Publish asynchronously
                        future = publisher.publish(topic_path, data=data_bytes, source="reddit_rss")
                        message_id = future.result()

                        print(f" Published to Pub/Sub! [Msg ID: {message_id}] | Title: {payload['post_title'][:60]}...")

            time.sleep(poll_interval_sec)

    except KeyboardInterrupt:
        print("\nPublisher stopped by user.")

if __name__ == "__main__":
    start_publishing_stream(poll_interval_sec=8)