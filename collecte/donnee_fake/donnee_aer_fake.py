import pandas as pd
from faker import Faker
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json, os, uuid, time
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra import OperationTimedOut

load_dotenv()
fake = Faker('fr_FR')

# ---------- KAFKA ----------
producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------- CASSANDRA ----------
# connexion au cluster Cassandra

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
        retry_delay = min(retry_delay * 1.2, 30)  
else:
    raise Exception(f"Impossible de se connecter à Cassandra après {max_retries * retry_delay}s")


# créer la keyspace
session.execute("""
CREATE KEYSPACE IF NOT EXISTS donnees_aer_fake
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

# utiliser la keyspace
session.set_keyspace('donnees_aer_fake')

# Création de la table (adapte les noms de colonnes à ton df_nosql)
session.execute("DROP TABLE IF EXISTS vol_fake")
session.execute("""
CREATE TABLE IF NOT EXISTS vol_fake (
    id UUID PRIMARY KEY,
    numero_vol text,
    airport_depart text,
    airport_arrivee text,
    pays_depart text,
    pays_destination text,
    compagnie text,
    heure_depart int,
    jour_semaine  INT,
    mois INT,
    saison text,
    fete_ou_non INT,
    delai_minutes INT,
    status TEXT,
    temperature float,
    pluie float,
    recommandation TEXT,
    generated_at TIMESTAMP
)
""")

# Supprimer et recréer le topic Kafka
admin_client = KafkaAdminClient(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'))
try:
    admin_client.delete_topics(['volFake_topic'])
    print("Topic Kafka supprimé.")
except Exception as e:
    print(f"Erreur suppression topic (peut-être inexistant) : {e}")
# topic = NewTopic(name='volFake_topic', num_partitions=1, replication_factor=1)
# admin_client.create_topics([topic])
# print("Topic Kafka recréé.")

# ---------- LISTE DES AEROPORTS ----------
aeroports= ['Aéroport International Cardinal Bernadin Gantin de Cotonou',
            'Aéroport International Thomas Sankara de Ouagadougou',
            'Aéroport International Blaise Diagne de Dakar','Aéroport Amîcar Cabral (Sal)',
            'Aéroport de Bobo-Dioulasso','Aéroport International Nelson Mandela (Praia)',
            'Aéroport International de Banjul','Aéroport International Osvaldo Vieira',
            'Aéroport International Ahmed Sékou Touré','Aéroport International Kotoka',
            'Aéroport de Kumasi','Aéroport International Roberts','Aéroport Spriggs Payne',
            'Aéroport International Modibo Keïta','Aéroport de Tombouctou','Aéroport International de Nouakchott-Oumtounsy',
            'Aéroport de Nouadhibou','Aéroport International Diori Hamani (Niamey)','Aéroport de Zinder','Aéroport Nnamdi Azikiwe',
            'Aéroport International Akanu Ibiam','Aéroport de Port Harcourt',
            'Aéroport International Murtala Muhammed de Lagos','Aéroport International Gnassingbé Eyadéma de Lomé']

# ---------- DESTINATIPAYSONS  ----------
pays_list = ['Sénégal','Burkina Faso','Bénin','Cap-Vert','Gambie','Ghana','Togo','Mali','Mauritanie']


def gerenerate_fake_vol_record():
    """
    Génère un record vol unique.
    """
    depart = random.choice(aeroports)
    arrivee = random.choice([a for a in aeroports if a != depart])

    heure = random.randint(0, 23)
    jour_semaine = datetime.now().weekday() + random.randint(-3, 3)
    mois = random.randint(1, 12)
    saison = "pluvieuse" if mois in [6,7,8,9] else "sèche"
    fete = 1 if random.random() < 0.15 else 0

    retard = random.choices([0, 15, 30, 60, 120, 300], weights=[60,20,10,6,3,1])[0]
    temp_arrivee = round(random.uniform(24, 42), 1)
    pluie = round(random.uniform(0, 20), 1)

    reco = []
    if pluie > 8:
        reco.append("véhicule fermé obligatoire")
    if retard > 60 and heure >= 22:
        reco.append("hôtel proche aéroport + chauffeur à l’arrivée")
    if temp_arrivee > 38:
        reco.append("eau + lieux climatisés recommandés")
    if fete == 1:
        reco.append("attention circulation dense")
    recommandation = " | ".join(reco) if reco else "voyage fluide"

    return {
        "id": str(uuid.uuid4()),
        "numero_vol": fake.bothify(text='??###'),
        "airport_depart": depart,
        "airport_arrivee": arrivee,
        "pays_depart": "Sénégal" if "Dakar" in depart else random.choice(pays_list),
        "pays_destination": arrivee.split()[-1][:-1] if arrivee.endswith(")") else arrivee.split()[-1],
        "compagnie": random.choice(['Air Senegal','ASky','Ethiopian Airlines','Turkish Airlines','Air Côte d Ivoire']),
        "heure_depart": heure,
        "jour_semaine": (jour_semaine % 7) + 1,
        "mois": mois,
        "saison": saison,
        "fete_ou_non": fete,
        "delai_minutes": retard,
        "status": "Delayed" if retard > 0 else "On Time",
        "temperature": temp_arrivee,
        "pluie": pluie,
        "recommandation": recommandation,
        "generated_at": datetime.now().isoformat()
    }

def generate_fake_vol_data(n=1000):
    """
    Génère n vols aléatoires pour CSV, JSON et Cassandra avec des données uniques.
    """
    # Supprimer les fichiers existants
    if os.path.exists('donnees_vol_Api.csv'):
        os.remove('donnees_vol_Api.csv')
    if os.path.exists('donnees_vol_Api.json'):
        os.remove('donnees_vol_Api.json')

    data_batch_csv = []
    data_batch_json = []

    for _ in range(n):
        # Générer pour CSV
        record_csv = gerenerate_fake_vol_record()
        data_batch_csv.append(record_csv)
        print(f"CSV - {record_csv['numero_vol']} → {record_csv['airport_depart']} | {record_csv['airport_arrivee']} | {record_csv['pays_depart']} | {record_csv['pays_destination']} | {record_csv['compagnie']}")

        # Générer pour JSON
        record_json = gerenerate_fake_vol_record()
        data_batch_json.append(record_json)
        print(f"JSON - {record_json['numero_vol']} → {record_json['airport_depart']} | {record_json['airport_arrivee']} | {record_json['pays_depart']} | {record_json['pays_destination']} | {record_json['compagnie']}")

        # Générer pour Cassandra et Kafka
        record_cass = gerenerate_fake_vol_record()
        #supprimer la partie kafka
        #producer.send('volFake_topic', value=record_cass)

        session.execute("""
        INSERT INTO vol_fake (
            id, numero_vol, airport_depart, airport_arrivee, pays_depart, pays_destination,
            compagnie, heure_depart, jour_semaine, mois, saison, fete_ou_non, delai_minutes, status,
            temperature, pluie, recommandation, generated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            uuid.UUID(record_cass["id"]), record_cass["numero_vol"], record_cass["airport_depart"], record_cass["airport_arrivee"],
            record_cass["pays_depart"], record_cass["pays_destination"], record_cass["compagnie"], record_cass["heure_depart"],
            record_cass["jour_semaine"], record_cass["mois"], record_cass["saison"], record_cass["fete_ou_non"], record_cass["delai_minutes"],
            record_cass["status"], record_cass["temperature"], record_cass["pluie"], record_cass["recommandation"],
            datetime.now()
        ))
        print(f"Cassandra - {record_cass['numero_vol']} → {record_cass['airport_depart']} | {record_cass['airport_arrivee']} | {record_cass['pays_depart']} | {record_cass['pays_destination']} | {record_cass['compagnie']}")

# ---------- GÉNÉRATION DE DONNÉES ----------
    df_csv = pd.DataFrame(data_batch_csv)
    df_csv.to_csv('donnees_vol_Api.csv', index=False)
    
    df_json = pd.DataFrame(data_batch_json)
    df_json.to_json('donnees_vol_Api.json', orient='records', lines=True, force_ascii=False)


if __name__ == "__main__":
    generate_fake_vol_data()
    producer.flush()
