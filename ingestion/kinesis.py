import json
import boto3
import os
from kafka import KafkaConsumer
from datetime import datetime
import time

kinesis = boto3.client('kinesis', region_name = 'eu-west-3')
STREAM_NAME = "AWSstream"

print(f"Connexion à Kinesis → Stream : {STREAM_NAME}")

consumer_vol = KafkaConsumer(
    'volApi_topic',
    bootstrap_servers='kafka:9092',
    group_id='kinesis-vol-group',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

consumer_meteo = KafkaConsumer(
    'weather_Api_topic',
    bootstrap_servers='kafka:9092',
    group_id='kinesis-meteo-group',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

while True:
    for consumer, source in [(consumer_vol, "VOL"), (consumer_meteo, "MÉTÉO")]:
        try:
            msg = next(consumer, None)
            if not msg:
                time.sleep(0.1)
                continue

            data = msg.value
            reco = []

            if source == "VOL":
                # Extraction heure arrivée
                scheduled = data.get("arrival_scheduled") or data.get("arrival", {}).get("scheduled", "")
                heure = scheduled[-8:-6] if scheduled and len(scheduled) >= 8 else "12"
                try:
                    heure_int = int(heure)
                except:
                    heure_int = 12
                is_night = heure_int < 6 or heure_int > 21

                status = str(data.get("flight_status", "")).lower()
                flight_iata = data.get("flight", {}).get("iata") or data.get("flight_number", "UNKNOWN")
                destination = data.get("arrival_iata") or data.get("arrival", {}).get("iata", "???")

                if data.get("pluie", 0) > 8 or "rain" in str(data).lower():
                    reco.append("véhicule fermé obligatoire")
                if is_night:
                    reco.append("nuit → hôtel sécurisé recommandé")
                if "delay" in status or "late" in status:
                    reco.append("retard → chauffeur ajusté")
                if data.get("temperature", 0) > 38:
                    reco.append("canicule → clim + eau")

                partition_key = flight_iata
                record = {
                    "type": "flight",
                    "flight_iata": flight_iata,
                    "destination": destination,
                    "is_night": is_night,
                    "status": data.get("flight_status"),
                    "recommandation_voyageur": " | ".join(reco) if reco else "voyage fluide",
                    "timestamp": datetime.now().isoformat()
                }

            else:  # MÉTÉO
                temp = data.get("temperature", 0)
                pluie = data.get("pluie", 0)
                ville = data.get("ville", "Inconnue")

                if pluie > 15:
                    reco.append("risque inondation")
                elif pluie > 8:
                    reco.append("pluie forte → véhicule fermé")
                if temp > 38:
                    reco.append("canicule → éviter 12h-16h")

                partition_key = ville
                record = {
                    "type": "weather",
                    "ville": ville,
                    "temperature": temp,
                    "pluie_mm": pluie,
                    "recommandation_voyageur": " | ".join(reco) if reco else "conditions normales",
                    "timestamp": datetime.now().isoformat()
                }

            # ENVOI VERS KINESISENIS
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(record, ensure_ascii=False).encode('utf-8'),
                PartitionKey=partition_key
            )

            key_display = record.get("flight_iata") or record.get("ville")
            print(f"KINESIS → {key_display:<12} | {record['recommandation_voyageur']:<50} | Seq: {response['SequenceNumber'][-8:]}")

        except Exception as e:
            print(f"Erreur Kinesis temporaire : {e}")
            time.sleep(1)

    time.sleep(0.1)