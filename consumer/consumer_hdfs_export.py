import os
import sys
import json
import time
from kafka import KafkaConsumer # <--- Ligne ajoutée pour définir KafkaConsumer
# NOTE: pyarrow.hdfs est désormais intégré dans pyarrow.fs.
# Utiliser pyarrow.fs pour l'accès HDFS est la méthode recommandée.
import pyarrow.fs as fs_mod

# ---------------------------
# CONFIGURATION
# ---------------------------
KAFKA_SERVER = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPIC_TRACKS = "tracks"

# Configuration HDFS 
HDFS_HOST = os.getenv("HDFS_HOST", "hadoop")
HDFS_PORT = int(os.getenv("HDFS_PORT", 9000))
# Chemin où le fichier sera créé sur le système de fichiers HDFS
HDFS_PATH = "/user/data/tracks/tracks_data.json" 

# Nombre de messages à lire pour le test (retirez cette limite en production)
MAX_MESSAGES_TO_READ = 10 

# ---------------------------
# INITIALISATION KAFKA
# ---------------------------
consumer = KafkaConsumer(
    TOPIC_TRACKS,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    group_id='hdfs_exporter',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"👀 Démarrage de l'export HDFS sur le topic : {TOPIC_TRACKS}")

# ---------------------------
# CONNEXION HDFS ET ÉCRITURE
# ---------------------------
count = 0
try:
    # 1. Connexion au système de fichiers HDFS (Utilise pyarrow.fs.HadoopFileSystem)
    # Ceci remplace l'ancien hdfs.connect() qui causait l'erreur d'importation.
    hdfs_fs = fs_mod.HadoopFileSystem(host=HDFS_HOST, port=HDFS_PORT)
    print(f"✅ Connexion réussie à HDFS sur hdfs://{HDFS_HOST}:{HDFS_PORT}")
    
    # 2. Ouvrir le fichier en mode écriture (crée ou écrase)
    # Notez l'utilisation de hdfs_fs
    with hdfs_fs.open_output_stream(HDFS_PATH) as hdfs_file:
        
        # 3. Traiter et écrire les messages du consommateur Kafka
        for message in consumer:
            data = message.value
            
            # Convertir le dictionnaire en ligne JSON (JSON Lines) et ajouter un saut de ligne
            json_line = json.dumps(data) + '\n'
            
            # Écriture dans le fichier HDFS (pyarrow requiert des bytes)
            hdfs_file.write(json_line.encode('utf-8'))
            
            print(f"✅ Écrit offset {message.offset} vers HDFS. Track ID: {data.get('track_id')}")
            
            count += 1
            if count >= MAX_MESSAGES_TO_READ:
                print(f"Limite de {MAX_MESSAGES_TO_READ} messages atteinte.")
                break
            
            # Petite pause pour ne pas surcharger HDFS/CPU pour ce test
            time.sleep(0.1)
                
except Exception as e:
    print(f"❌ ERREUR D'ÉCRITURE/CONSOMMATION: {e}")
finally:
    # Assurez-vous que le consumer est fermé
    consumer.close() 

print(f"\nExport HDFS terminé. {count} messages traités et écrits dans {HDFS_PATH}")
