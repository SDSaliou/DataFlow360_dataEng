import json
import os
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.cloud import pubsub_v1
from dotenv import load_dotenv

load_dotenv()

# ==================== CONFIG ====================
PROJET = os.getenv("GCP_PROJECT_ID", "dataflow-360")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Topics Kafka à écouter
KAFKA_TOPICS = {
    "volApi_topic": ("flights-raw", "flight"),
    "weather_Api_topic": ("weather-raw-topic", "weather")
}

# ==================== PUB/SUB CLIENT ====================
publisher = pubsub_v1.PublisherClient()

# Création sécurisée des topic paths
topic_paths = {
    name: publisher.topic_path(PROJET, pubsub_topic)
    for name, (pubsub_topic, _) in KAFKA_TOPICS.items()
}

# ==================== KAFKA CONSUMERS ====================
consumers = []
for kafka_topic, (pubsub_topic, source) in KAFKA_TOPICS.items():
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='latest',           
        enable_auto_commit=True,
        group_id=f'pubsub-bridge-{source}',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000  
    )
    consumers.append((consumer, topic_paths[kafka_topic], source))

print(f"Bridge Kafka → Pub/Sub démarré | Projet: {PROJET}")
print(f"Écoute sur : {list(KAFKA_TOPICS.keys())}")
print(f"Envoi vers   : {list(topic_paths.values())}")

# ==================== FONCTION D'ENRICHISSEMENT ====================
def enrich_message(data: dict, source: str) -> dict:
    reco = []

    if source == "flight":
        # Extraction heure arrivée
        scheduled = data.get("arrival", {}).get("scheduled") or data.get("arrival_scheduled", "")
        heure = scheduled[-8:-6] if scheduled and len(scheduled) >= 8 else "12"
        try:
            heure_int = int(heure)
        except:
            heure_int = 12
        is_night = heure_int < 6 or heure_int > 21
        status = str(data.get("flight_status", "")).lower()

        if data.get("pluie", 0) > 8:
            reco.append("véhicule fermé obligatoire")
        if is_night:
            reco.append("nuit → hôtel sécurisé recommandé")
        if "delay" in status or "late" in status:
            reco.append("retard → chauffeur ajusté")
        if data.get("temperature", 0) > 38:
            reco.append("canicule → clim + eau")

        return {
            "type": "flight_enriched",
            "flight_iata": data.get("flight", {}).get("iata", "???"),
            "arrival_iata": data.get("arrival", {}).get("iata", "???"),
            "airport": data.get("arrival", {}).get("airport"),
            "status": data.get("flight_status"),
            "is_night_arrival": is_night,
            "recommandation_voyageur": " | ".join(reco) if reco else "voyage fluide",
            "ingestion_timestamp": datetime.now().isoformat() + "Z",
            "source": "aviationstack"
        }

    else:  # weather
        temp = data.get("temperature", 0)
        pluie = data.get("pluie", 0) or data.get("pluie_1h", 0)
        humidite = data.get("humidite", 0)

        if pluie > 15: reco.append("risque inondation → éviter zones basses")
        elif pluie > 8: reco.append("pluie forte → véhicule fermé")
        if temp > 38: reco.append("canicule → éviter 12h-16h")
        if humidite > 90: reco.append("humidité extrême → clim obligatoire")

        return {
            "type": "weather_enriched",
            "ville": data.get("ville"),
            "temperature": temp,
            "pluie_1h": pluie,
            "humidite": humidite,
            "condition": data.get("condition"),
            "recommandation_voyageur": " | ".join(reco) if reco else "conditions idéales",
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "source": "openweather"
        }

# ==================== BOUCLE PRINCIPALE ====================
while True:
    try:
        for consumer, topic_path, source in consumers:
            try:
                msg = consumer.poll(timeout_ms=1000)
                if not msg:
                    continue

                for topic_partition, messages in msg.items():
                    for message in messages:
                        raw_data = message.value
                        enriched = enrich_message(raw_data, source)

                        # Envoi avec retry automatique
                        future = publisher.publish(
                            topic_path,
                            json.dumps(enriched, ensure_ascii=False).encode("utf-8"),
                            source=source,
                            ingestion_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                        )
                        message_id = future.result(timeout=10)

                        key = enriched.get("flight_iata") or enriched.get("ville")
                        print(f"PUB/SUB → {key:12} | {enriched['recommandation_voyageur']:40} | ID: {message_id}")

                consumer.commit()
            except Exception as e:
                print(f"Erreur traitement {source}: {e}")
                time.sleep(2)

        time.sleep(0.1)  

    except KeyboardInterrupt:
        print("\nArrêt du bridge Kafka → Pub/Sub")
        break
    except Exception as e:
        print(f"Erreur critique: {e}")
        time.sleep(5)