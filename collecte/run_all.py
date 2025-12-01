import subprocess
import time

print("Lancement de tous les producteurs...")
processes = [
    subprocess.Popen(["python", "donnee_fake/donnee_aer_fake.py"]),
    subprocess.Popen(["python", "donnee_fake/donnee_meteo_fake.py"]),
    subprocess.Popen(["python", "donnee_api/apiVols_produceer.py"]),
    subprocess.Popen(["python", "donnee_api/apiMeteo.py"]),
]

print("TOUS LES PRODUCTEURS LANCÉS !")
print("Topics Kafka actifs : flights_synthetic_topic, weather_topic, flights_real_topic")
time.sleep(999999)  # garder le script en vie pour que les processus enfants restent actifs