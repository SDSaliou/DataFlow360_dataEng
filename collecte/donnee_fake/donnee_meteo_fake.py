import pandas as pd
from faker import Faker
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json, os, uuid, time
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

load_dotenv()
fake = Faker('fr_FR')

# ---------- KAFKA ----------
producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------- CASSANDRA ----------
print("Attente démarrage Cassandra...")
time.sleep(35)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
max_retries = 30
retry_delay = 5

print("Attente démarrage Cassandra...")
time.sleep(35)

cluster = None
session = None

for attempt in range(max_retries):
    try:
        cluster = Cluster([CASSANDRA_HOST], port=9042)
        session = cluster.connect()
        print(f"Connexion à Cassandra réussie à la tentative {attempt + 1}")
        break
    except Exception as e:
        print(f"Tentative {attempt + 1}/{max_retries} - Cassandra pas encore prête... (Erreur: {str(e)})")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 1.2, 30)  # Recul exponentiel, plafonné à 30s
else:
    raise Exception(f"Impossible de se connecter à Cassandra après {max_retries * retry_delay}s")


session.execute("""
CREATE KEYSPACE IF NOT EXISTS meteo_fake 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace('meteo_fake')

session.execute("DROP TABLE IF EXISTS meteo_fake")

session.execute("""
CREATE TABLE IF NOT EXISTS meteo_fake (
    id UUID PRIMARY KEY,
    ville TEXT,
    temperature FLOAT,
    humidite INT,
    pression INT,
    vent_vitesse FLOAT,
    precipitation FLOAT,
    nebulosite FLOAT,
    visibilite INT,
    point_de_rosee FLOAT,
    condition_meteo TEXT,
    indice_meteo_global INT,
    vent_direction TEXT,
    pluie FLOAT,
    is_raining BOOLEAN,
    is_hot BOOLEAN,
    is_night BOOLEAN,
    recommandation TEXT,
    date_mesure TIMESTAMP
)
""")

# Supprimer et recréer le topic Kafka
admin_client = KafkaAdminClient(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'))
try:
    admin_client.delete_topics(['weatherFake_topic'])
    print("Topic Kafka supprimé.")
except Exception as e:
    print(f"Erreur suppression topic (peut-être inexistant) : {e}")
topic = NewTopic(name='weatherFake_topic', num_partitions=1, replication_factor=1)
admin_client.create_topics([topic])
print("Topic Kafka recréé.")


villes_ouest_afrique = ["Dakar", "Ouagadougou", "Bamako", "Abidjan", "Conakry", "Niamey", "Lomé", "Cotonou"]

def generate_weather_record():
    """
    Génère un record météo unique.
    """
    heure = datetime.now().hour
    is_night = heure < 6 or heure > 21
    temperature = round(random.uniform(24, 43), 1)
    pluie = round(random.uniform(0, 25), 1)
    humidite = random.randint(45, 98)
    vent = round(random.uniform(0, 35), 1)
    
    #------------- recommandation --------------
    reco = []
    if pluie > 8:
        reco.append("pluie forte → vehicule ferme obligatoire")
    if pluie > 15:
        reco.append("risque d inondation → eviter les routes basses")
    if temperature > 38:
        reco.append("canicule → eau + lieux climatises + eviter 12h-16h")
    if humidite > 90:
        reco.append("tres lourd → climatisation recommandee")
    if vent > 25:
        reco.append("vent violent → attention aux objets volants")
    if is_night and pluie > 5:
        reco.append("nuit + pluie → chauffeur local fortement conseille")
    if temperature < 28 and pluie < 1:
        reco.append("temps ideal → parfait pour visiter !")
    
    recommandation = " | ".join(reco) if reco else "Conditions normales"
    
    return {
        "id": str(uuid.uuid4()),
        "ville": random.choice(villes_ouest_afrique),  
        "temperature": temperature,
        "humidite": humidite,
        "pression": random.randint(1005, 1020),
        "vent_vitesse": vent,
        "precipitation": round(random.uniform(0, 30), 1),
        "nebulosite": round(random.uniform(0, 100), 1),
        "visibilite": random.randint(800, 10000),
        "point_de_rosee": round(temperature - (100 - humidite)/5, 1),
        "condition_meteo": "Pluvieux" if pluie > 5 else "Ensoleillé" if temperature > 35 else "Nuageux",
        "indice_meteo_global": 1 if pluie > 10 or temperature > 40 else 0,
        "vent_direction": random.choice(['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']),
        "pluie": pluie,
        "is_raining": pluie > 3,
        "is_hot": temperature > 38,
        "is_night": is_night,
        "recommandation": recommandation,
        "date_mesure": datetime.now().isoformat()
    }

def generate_fake_weather_data(n=1000):
    """
    Génère n mesures météo aléatoires pour CSV, JSON et Cassandra avec des données uniques.
    """
    # Supprimer les fichiers existants
    if os.path.exists('donnees_meteo_Fake.csv'):
        os.remove('donnees_meteo_Fake.csv')
    if os.path.exists('donnees_meteo_Fake.json'):
        os.remove('donnees_meteo_Fake.json')
    
    data_batch_csv = []
    data_batch_json = []
    
    for _ in range(n):
        # Générer pour CSV
        record_csv = generate_weather_record()
        data_batch_csv.append(record_csv)
        print(f"CSV - {record_csv['ville']} → {record_csv['temperature']}°C | Pluie: {record_csv['pluie']}mm | {record_csv['recommandation'][:60]}...")
        
        # Générer pour JSON
        record_json = generate_weather_record()
        data_batch_json.append(record_json)
        print(f"JSON - {record_json['ville']} → {record_json['temperature']}°C | Pluie: {record_json['pluie']}mm | {record_json['recommandation'][:60]}...")
        
        # Générer pour Cassandra/Kafka
        record_cass = generate_weather_record()
        producer.send('weatherFake_topic', value=record_cass)
        session.execute("""
        INSERT INTO meteo_fake (
            id, ville, temperature, humidite, pression, vent_vitesse, precipitation,
            nebulosite, visibilite, point_de_rosee, condition_meteo, indice_meteo_global,
            vent_direction, pluie, is_raining, is_hot, is_night, recommandation, date_mesure
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            uuid.UUID(record_cass["id"]), record_cass["ville"], record_cass["temperature"], record_cass["humidite"],
            record_cass["pression"], record_cass["vent_vitesse"], record_cass["precipitation"], record_cass["nebulosite"],
            record_cass["visibilite"], record_cass["point_de_rosee"], record_cass["condition_meteo"],
            record_cass["indice_meteo_global"], record_cass["vent_direction"], record_cass["pluie"],
            record_cass["is_raining"], record_cass["is_hot"], record_cass["is_night"],
            record_cass["recommandation"], datetime.now()
        ))
        print(f"Cassandra/Kafka - {record_cass['ville']} → {record_cass['temperature']}°C | Pluie: {record_cass['pluie']}mm | {record_cass['recommandation'][:60]}...")
    
    # Sauvegarde
    df_csv = pd.DataFrame(data_batch_csv)
    df_csv.to_csv('donnees_meteo_Fake.csv', index=False)
    
    df_json = pd.DataFrame(data_batch_json)
    df_json.to_json('donnees_meteo_Fake.json', orient='records', lines=True, force_ascii=False)

if __name__ == "__main__":
    generate_fake_weather_data()
    producer.flush()