import json
import time
import os
import psycopg2
from kafka import KafkaConsumer

# 1. Configuration des paramètres
TOPIC_NAME = 'deezer_artists_raw'

# Paramètres de connexion Kafka
KAFKA_BROKER = 'kafka:9092' 

# Paramètres de connexion PostgreSQL (récupérés des variables d'environnement)
DB_HOST = 'postgres'
DB_NAME = os.environ.get('POSTGRES_DB', 'datapipeline') 
DB_USER = os.environ.get('POSTGRES_USER', 'admin')
DB_PASS = os.environ.get('POSTGRES_PASSWORD', 'admin')
TABLE_NAME = 'deezer_artists'

# 2. Connexion à PostgreSQL
def get_db_connection():
    # Tente de se connecter, avec une petite attente pour garantir la disponibilité de PostgreSQL
    max_retries = 5
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            print("Connexion à PostgreSQL réussie.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Tentative {i+1} : Échec de la connexion à PostgreSQL. Attente de 5 secondes... {e}")
            time.sleep(5)
    raise Exception("Impossible de se connecter à la base de données après plusieurs tentatives.")

# 3. Fonction de Traitement et Insertion
def process_message(conn, data):
    # Les données brutes de Kafka sont des chaînes (y compris les nombres). 
    # Nous devons convertir les types pour PostgreSQL.

    try:
        # Conversion des chaînes en types numériques
        artist_id = int(data.get('id'))
        fan_count = int(data.get('nb_fan'))
        
        # Le nom (name) est déjà une chaîne
        name = data.get('name')
        link = data.get('link')
        picture = data.get('picture')

    except (ValueError, TypeError) as e:
        print(f"Erreur de conversion de type pour l'enregistrement ID {data.get('id')}. Ignoré. Erreur: {e}")
        return

    # Commande SQL pour l'insertion
    sql = f"""
    INSERT INTO {TABLE_NAME} (id, name, nb_fan, link, picture)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """
    
    try:
        cur = conn.cursor()
        cur.execute(sql, (artist_id, name, fan_count, link, picture))
        conn.commit()
        print(f"Artiste ID {artist_id} inséré/mis à jour dans {TABLE_NAME}.")
        cur.close()
    except Exception as e:
        print(f"ERREUR D'INSERTION SQL pour l'artiste {artist_id}: {e}")
        conn.rollback()


# 4. Initialisation du Consommateur
def start_consumer():
    conn = get_db_connection()
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest', # Commence à lire depuis le début du topic
        enable_auto_commit=True,
        group_id='artist-processor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Consommateur démarré sur le topic {TOPIC_NAME}. En attente de messages...")

    for message in consumer:
        # message.value contient les données JSON désérialisées
        data = message.value
        process_message(conn, data)

    conn.close()

if __name__ == "__main__":
    start_consumer()