from kafka import KafkaConsumer
from google.cloud import pubsub_v1
import json, os, time
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

projet = "dataflow-360"
topic_vol ="flights_raw"
topic_meteo ="weather_raw"

publisher = pubsub_v1.PublisherClient()
topic_path_vol = publisher.topic_path(projet, topic_vol)
topic_path_meteo = publisher.topic_path(projet, topic_meteo)

consumer_vol = KafkaConsumer(
    'volApi_topic',
    bootstrap_servers="kafka:9092",
    auto_offset_reset='latest',
    group_id='pubsub-vol-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

consumer_meteo = KafkaConsumer(
    'weather_Api_topic',
    bootstrap_servers="kafka:9092",
    auto_offset_reset='latest',
    group_id='pubsub-meteo-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Envoi réel vers google pub/sub {topic_vol} et {topic_meteo}")

while True:
    for consumer, topic_path, source in [
        (consumer_vol, topic_path_vol, "vols"),
        (consumer_meteo, topic_path_meteo, "meteo")
    ]:
        message = next(consumer, None)
        if not message:
            continue

        data = message.value
        reco = []

            
        if source == "vols":
            scheduled = data.get("arrival", {}).get("scheduled", "") or data.get("arrival_scheduled", "")
            heure = scheduled[-8:-6] if scheduled and len(scheduled) >= 8 else "12"
            try:
                heure_int = int(heure)
            except:
                heure_int = 12
            is_night = heure_int < 6 or heure_int > 21
            status = str(data.get("flight_status", "")).lower()

            if data.get("pluie", 0) > 8 or "rain" in str(data).lower():
                reco.append("véhicule fermé obligatoire")
            if is_night:
                reco.append("nuit → hôtel sécurisé recommandé")
            if "delayed" in status or "late" in status:
                reco.append("retard → chauffeur ajusté")
            if data.get("temperature", 0) > 38:
                reco.append("canicule → clim + eau")

            enriched = {
                "type": "flight",
                "flight_iata": data["flight"]["iata"],
                "arrival_iata": data["arrival"]["iata"],
                "airport": data["arrival"]["airport"],
                "status": data.get("flight_status"),
                "recommandation_voyageur": " | ".join(reco) if reco else "voyage fluide",
                "is_night_arrival": is_night,
                "timestamp": datetime.now().isoformat(),
                "source": "aviationstack"
            }

        else:  # météo
            temp = data.get("temperature", 0)
            pluie = data.get("pluie", 0)
            
            if pluie > 8: reco.append("pluie forte → véhicule fermé")
            if pluie > 15: reco.append("risque inondation")
            if temp > 38: reco.append("canicule → éviter 12h-16h")
            if data.get("humidite", 0) > 90: 
                reco.append("humidité extrême")
            
            enriched = {
                "type": "weather",
                "ville": data["ville"],
                "temperature": temp,
                "pluie": pluie,
                "humidite": data.get("humidite"),
                "condition": data.get("condition"),
                "recommandation_voyageur": " | ".join(reco) if reco else "conditions normales",
                "timestamp": datetime.now().isoformat(),
                "source": "openweather"
            }

        # ENVOI RÉEL VERS PUB/SUB
        data_bytes = json.dumps(enriched, ensure_ascii=False).encode("utf-8")
        future = publisher.publish(topic_path, data_bytes)
        msg_id = future.result()  # attend confirmation
        
        key = enriched.get("flight_iata") or enriched.get("ville")
        print(f"PUB/SUB → {key} | {enriched['recommandation_voyageur']} | ID: {msg_id}")