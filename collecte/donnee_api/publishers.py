import json
import os
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
project_id = os.getenv("GCP_PROJECT_ID", "dataflow-360")

def publish_everywhere(topic_name: str, data: dict):
    topic_path = publisher.topic_path(project_id, topic_name)
    message_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    future = publisher.publish(topic_path, data=message_bytes)
    message_id = future.result(timeout=10)
    ville = data.get("ville", "Inconnu")
    print(f"TEST CI/CD → {ville} → {topic_name} | ID: {message_id}")
    return message_id