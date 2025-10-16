import csv
import json
import time
from kafka import KafkaProducer
import os # Ajout pour vérifier l'existence du fichier

# 1. Configuration des paramètres

# Adresse du broker Kafka (utilise le nom de service Docker)
KAFKA_BROKER = 'kafka:9092' 

# Topic pour les données brutes des albums
TOPIC_NAME = 'deezer_albums_raw'
# Nom du fichier CSV
FILE_PATH = '/app/albums.csv' 

# 2. Initialisation du Producteur
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        # Fonction de sérialisation JSON
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    print(f"Producteur d'albums connecté à Kafka sur {KAFKA_BROKER}")

except Exception as e:
    print(f"ERREUR DE CONNEXION À KAFKA : {e}")
    exit(1)  # Quitte si la connexion échoue

# 3. Lecture du fichier CSV et envoi des messages
if not os.path.exists(FILE_PATH):
    print(f"ERREUR : Le fichier {FILE_PATH} est introuvable. Assurez-vous qu'il est dans le répertoire de l'application.")
else:
    print(f"Lecture du fichier {FILE_PATH} et envoi au topic {TOPIC_NAME}...")

    # Utilisation de 'with' pour garantir la fermeture du fichier
    with open(FILE_PATH, mode='r', encoding='utf-8') as csvfile:
        # Utilise DictReader pour lire chaque ligne comme un dictionnaire
        reader = csv.DictReader(csvfile)
        
        # Lecture ligne par ligne
        for row in reader:
            data_json = row
            
            # Utilise l'album_id comme clé pour le partitionnement (bonne pratique)
            key = str(data_json.get('album_id')).encode('utf-8') 
            
            # Envoi asynchrone du message
            future = producer.send(TOPIC_NAME, key=key, value=data_json)
            
            try:
                record_metadata = future.get(timeout=10)
                # Affichage de confirmation
                print(f"Envoyé album ID {data_json['album_id']} à la partition {record_metadata.partition}")
                
            except Exception as e:
                print(f"Échec de l'envoi pour l'album {data_json.get('album_id', 'N/A')}: {e}")
                
            # Simule un flux continu (un message toutes les 0.1 seconde)
            time.sleep(0.1)

# Fermer la connexion du producteur
producer.close()
print("Producteur d'albums terminé.")