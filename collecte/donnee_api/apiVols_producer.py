import requests, json, time, os
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

API_KEY = os.getenv("AVIATIONSTACK_KEY") 
AIRPORTS = ["DSS", "OUA", "BKO", "ABJ", "CKY", "NIM", "LFW", "COO"] 

while True:
    for iata in AIRPORTS:
        url = f"http://api.aviationstack.com/v1/flights?access_key={API_KEY}&arr_iata={iata}&limit=5"
        
        try:
            response = requests.get(url, timeout=10)

            if response.status_code != 200:
                print(f"Erreur HTTP {response.status_code} pour {iata} : {response.text[:200]}")
                time.sleep(2)
                continue

            data = response.json()

            if 'error' in data:
                print(f"Erreur dans la réponse API pour {iata} :", data['error'])
                time.sleep(2)
                continue
            
            flights = data.get('data', [])
            print(f"{iata} → {len(flights)} vols trouvés")

            for vol in flights:
                departure = vol.get("departure") or {}
                arrival = vol.get("arrival") or {}
                airline = vol.get("airline") or {}
                flight = vol.get("flight") or {}
                aircraft = vol.get("aircraft") or {}

                rd = {
                    "flight_date": vol.get("flight_date"),
                    "flight_status": vol.get("flight_status"),
                    "departure": {
                        "airport": departure.get("airport"),
                        "iata": departure.get("iata"),
                        "scheduled": departure.get("scheduled"),
                        "actual": departure.get("actual"),
                        "terminal": departure.get("terminal"),
                        "gate": departure.get("gate"),
                    },
                    "arrival": {
                        "airport": arrival.get("airport"),
                        "iata": arrival.get("iata"),
                        "scheduled": arrival.get("scheduled"),
                        "actual": arrival.get("actual"),
                        "terminal": arrival.get("terminal"),
                        "gate": arrival.get("gate"),
                    },
                    "airline": airline.get("name"),
                    "flight": {
                        "iata": flight.get("iata"),
                        "icao": flight.get("icao"),
                        "number": flight.get("number"),
                    },
                    "aircraft_icao24": aircraft.get("icao24"),
                    "ingestion_timestamp": time.time(),
                    "source": "aviationstack",
                    "arr_iata": iata
                }

                producer.send('volApi_topic', value=rd)
                flight_iata = rd["flight"]["iata"] or "???"
                print(f"Envoi Kafka → {flight_iata} → {iata} ({rd['flight_status'] or 'inconnu'})")            
                time.sleep(2)

        except requests.exceptions.RequestException as e:
            print(f"Erreur réseau pour {iata} :", e)
        except Exception as e:
            print("Erreur inattendue :", e)
    
    print("Pause de 60 secondes avant prochain tour...")
    time.sleep(60)