import json
from google.cloud import pubsub_v1

PROJECT_ID = "cinepulse-analytics"
SUBSCRIPTION_ID = "reddit-movie-stream-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    print(f"\n[Received Message ID: {message.message_id}]")
    print(f"Subreddit : r/{data.get('subreddit')}")
    print(f"Title     : {data.get('post_title')}")
    print(f"Ingested  : {data.get('ingestion_timestamp')}")
    
    # Acknowledge the message so it is cleared from the queue
    message.ack()

print(f"Listening for messages on {subscription_path} (Press Ctrl+C to exit)...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\nSubscriber stopped.")