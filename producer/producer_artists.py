import csv
import json
import time
from kafka import KafkaProducer

# 1. Configuration des paramètres

# Adresse du broker Kafka (utilise le nom de service Docker)
KAFKA_BROKER = 'kafka:9092' 

# Topic pour les données brutes des artistes
TOPIC_NAME = 'deezer_artists_raw'
# Nom du fichier CSV
FILE_PATH = '/app/artists.csv' 

# 2. Initialisation du Producteur
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        # Fonction de sérialisation JSON
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    print(f"Producteur connecté à Kafka sur {KAFKA_BROKER}")

except Exception as e:
    print(f"ERREUR DE CONNEXION À KAFKA : {e}")
    exit(1)  # Quitte si la connexion échoue

# 3. Lecture du fichier CSV et envoi des messages
print(f"Lecture du fichier {FILE_PATH} et envoi au topic {TOPIC_NAME}...")

# Utilisation de 'with' pour garantir la fermeture du fichier
with open(FILE_PATH, mode='r', encoding='utf-8') as csvfile:
    # Utilise DictReader pour lire chaque ligne comme un dictionnaire (clé=nom de colonne)
    reader = csv.DictReader(csvfile)
    
    # Lecture ligne par ligne
    for row in reader:
        # La ligne est déjà sous forme de dictionnaire Python
        data_json = row
        
        # Envoi asynchrone du message
        future = producer.send(TOPIC_NAME, value=data_json)
        
        try:
            # force l'attente pour confirmer l'envoi (peut être retiré pour plus de vitesse)
            record_metadata = future.get(timeout=10)
            
            # Affichage de confirmation pour la première étape
            print(f"Envoyé: {data_json['id']} à la partition {record_metadata.partition}")
            
        except Exception as e:
            print(f"Échec de l'envoi pour l'artiste {data_json.get('id', 'N/A')}: {e}")
            
        # Simule un flux continu (un message toutes les 0.5 seconde)
        time.sleep(0.5)

# Fermer la connexion du producteur
producer.close()
print("Producteur terminé.")