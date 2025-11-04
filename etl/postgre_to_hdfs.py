import pandas as pd
import psycopg2
import time
import requests
import io

# --- Configuration ---
POSTGRES = {
    "host": "postgres",
    "database": "datapipeline",
    "user": "admin",
    "password": "admin"
}

HDFS_URL = "http://namenode:9870/webhdfs/v1"
HDFS_USER = "root"
TABLES = ["artists", "albums", "tracks"]  # tables à exporter


# --- Étape 1 : Attendre que PostgreSQL soit prêt ---
def wait_for_postgres():
    for _ in range(10):
        try:
            conn = psycopg2.connect(**POSTGRES)
            conn.close()
            print("PostgreSQL est prêt.")
            return
        except Exception as e:
            print("En attente de PostgreSQL...", e)
            time.sleep(3)
    raise RuntimeError("PostgreSQL inaccessible.")


# --- Étape 2 : Lire une table PostgreSQL ---
def extract_table(table_name):
    conn = psycopg2.connect(**POSTGRES)
    df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    conn.close()
    print(f"{len(df)} lignes extraites de '{table_name}'.")
    return df


# --- Étape 3 : Envoyer une table vers HDFS via WebHDFS ---
def upload_to_hdfs(df, table_name):
    hdfs_path = f"/data/{table_name}.csv"

    # Convertit en CSV en mémoire
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    print(f"Upload de '{table_name}' vers HDFS ({hdfs_path})...")

    # Étape 1 : demander une URL de redirection
    create_url = f"{HDFS_URL}{hdfs_path}?op=CREATE&overwrite=true&user.name={HDFS_USER}"
    resp = requests.put(create_url, allow_redirects=False)
    if "Location" not in resp.headers:
        print(f"Erreur création {table_name}: {resp.text}")
        return
    redirect_url = resp.headers["Location"]

    # Étape 2 : envoyer le contenu réel
    put_resp = requests.put(redirect_url, data=csv_data.encode("utf-8"))
    if put_resp.status_code == 201:
        print(f"'{table_name}.csv' chargé avec succès dans HDFS !")
    else:
        print(f"Erreur upload {table_name}: {put_resp.text}")


# --- Main ---
if __name__ == "__main__":
    wait_for_postgres()

    for table in TABLES:
        try:
            df = extract_table(table)
            upload_to_hdfs(df, table)
        except Exception as e:
            print(f"Erreur lors du traitement de {table}: {e}")
