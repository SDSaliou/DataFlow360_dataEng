# Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY collecte/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Google Cloud SDK pour Pub/Sub
RUN apt-get update && apt-get install -y gcc python3-dev
RUN pip install google-cloud-pubsub boto3 kafka-python python-dotenv requests

COPY . .

CMD ["tail", "-f", "/dev/null"]