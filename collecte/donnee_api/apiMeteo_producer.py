import requests, json, time, os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP","kafka:9092"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

API_KEY = os.getenv("OPENWEATHER_KEY") 
if not API_KEY:
    raise ValueError("Clé API OpenWeather non définie dans les variables d'environnement.")
CITIES = ["Dakar", "Ouagadougou", "Bamako", "Abidjan", "Accra"]

while True:
    for city in CITIES:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric&lang=fr"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                print(f"Erreur HTTP {r.status_code} pour {city} : {r.json().get('message', 'inconnu')}")
                time.sleep(8)
                continue
            
            meteo = r.json()
            ord_data = {
                "ville": city,
                "temperature": meteo["main"]["temp"],
                "ressenti": meteo["main"].get("feels_like"),
                "temperature_min": meteo["main"]["temp_min"],
                "temperature_max": meteo["main"]["temp_max"],
                "humidite": meteo["main"]["humidity"],
                "pression": meteo["main"]["pressure"],
                "vent_vitesse": meteo["wind"].get("speed", 0),
                "vent_direction": meteo["wind"].get("deg"),
                "vent_rafales": meteo["wind"].get("gust"),
                "pluie": meteo.get("rain", {}).get("1h", 0.0),
                "neige": meteo.get("snow", {}).get("1h", 0.0),
                "nebulosite": meteo["clouds"]["all"],
                "visibilite": meteo.get("visibility", 10000),
                "condition": meteo["weather"][0]["description"],
                "indice_meteo_global": meteo["weather"][0]["id"],
                "lever_soleil": meteo["sys"]["sunrise"],
                "coucher_soleil": meteo["sys"]["sunset"],
                "timezone_offset": meteo["timezone"],
                "ingestion_timestamp": time.time(),
                "source": "openweather"
            }
            producer.send("weather_Api_topic", value=ord_data)
            print(f"Météo → {city}: {ord_data['temperature']:.1f}°C | "
                  f"Ressenti {ord_data['ressenti']:.1f}°C | "
                  f"Pluie: {ord_data['pluie']}mm | "
                  f"{ord_data['condition'].capitalize()}")

            time.sleep(8)  

        except requests.exceptions.RequestException as e:
            print(f"Erreur réseau pour {city} :", e)
        except Exception as e:
            print(f"Erreur inattendue pour {city} :", e)

    print("Pause de 60 secondes avant le prochain tour...\n")
    time.sleep(60)