📘 Data Pipeline MVP – Hamid / Stéphanie / Assaa

Projet Ynov visant à construire un pipeline de données complet incluant ingestion en streaming, stockage, export big data, traitement MapReduce et analyse via Hive.

🏗️ Architecture du pipeline
📌 Architecture Phase 1 (Kafka → Consumer → PostgreSQL)
Producer (Kafka) --> Kafka Broker --> Consumer Python --> PostgreSQL

Description

Le Producer génère des événements musicaux (tracks, genres, artistes…)

Kafka sert de bus de streaming

Le Consumer lit les messages et les insère dans PostgreSQL

PostgreSQL centralise les données brutes structurées

📌 Architecture Phase 2 (PostgreSQL → HDFS → MapReduce → Hive)
PostgreSQL --> Java Export --> HDFS --> MapReduce --> Output HDFS --> Hive

Description

Java exporte une requête SQL vers HDFS (TSV)

Un job MapReduce calcule le Top Tracks par genre

Hive lit:

les données d’entrée

les résultats MapReduce

📂 Structure du projet
DATA-PIPELINE-MVP/

├── Dockerfile
├── requirements.txt
│ 
├── consumer/
│   └── consumer.py
├── producer/
│   └── producer.py
│   
│
├── db/
│   └── init.sql
│
├── docker/
│   ├── docker-compose.yml
│   ├── docker-compose-phase2.yml
│   ├── hadoop-hive.env
│   └── processing-java/
│       ├── pom.xml
│       └── src/main/java/com/ynov/pipeline/
│           ├── ExportToHdfs.java
│           ├── GenreTopTracksJob.java
│           ├── GenreTopTracksMapper.java
│           └── GenreTopTracksReducer.java
│
└── docs/
    ├── architecture.drawio
    ├── phase1-architecture.drawio
    └── phase1-architecture.drawio.png

🗃️ Tables PostgreSQL – Description complète

Voici les tables créées dans init.sql, avec leur rôle fonctionnel.

1. genres

Représente les genres musicaux.

Colonne	Type	Description
genre_id	SERIAL PK	Identifiant unique du genre
name	TEXT	Nom du genre (ex: Pop, Rap, Jazz)

🎯 Rôle : Dictionnaire des genres utilisés partout dans le pipeline.

2. artists

Représente les artistes associés aux morceaux.

Colonne	Type	Description
artist_id	SERIAL PK	Identifiant de l'artiste
name	TEXT	Nom de l'artiste

🎯 Rôle : Dictionnaire des artistes, lié aux tracks.

3. tracks

Représente les morceaux musicaux.

Colonne	Type	Description
track_id	SERIAL PK	Identifiant du morceau
artist_id	INT FK	Référence à artists.artist_id
title	TEXT	Titre du morceau

🎯 Rôle : Ensemble des morceaux pouvant apparaître dans les classements.

4. chart_snapshots

Représente l’évolution d’un classement (ranking) dans le temps.

Colonne	Type	Description
snapshot_id	SERIAL PK	Identifiant du snapshot
snapshot_time	TIMESTAMP	Date du classement
track_id	INT FK	Référence au morceau classé
ref_genre_id	INT FK	Référence au genre
position	INT	Classement (1 = meilleur score)

🎯 Rôle :

Stocke les classements temporels

Données principales pour MapReduce

Utilisé dans l’export Postgres → HDFS

🚀 Lancer le pipeline
1️⃣ Phase 1 – Kafka → PostgreSQL

Dans docker/ :

docker compose up --build


Kafka + Postgres + Producer + Consumer démarrent.

Pour vérifier les données :

docker exec -it postgres psql -U Projet_MVP2025 datapipeline
SELECT * FROM tracks LIMIT 10;

2️⃣ Phase 2 – Hadoop, Hive, MapReduce

Démarrer Hadoop + Hive :

docker compose -f docker-compose-phase2.yml up --build

📌 Build du Java (Export + MapReduce)

Tu dois utiliser Maven dans un conteneur :

cd docker/processing-java
WINPATH=$(pwd -W)

docker run --rm -it \
  -v "$WINPATH":/app \
  -w /app \
  maven:3.9.6-eclipse-temurin-11 \
  mvn clean package


JAR généré :

target/processing-java-1.0.0.jar

📌 Export PostgreSQL → HDFS

Entrer dans le NameNode :

docker exec -it namenode bash


Exécuter l'export :

hadoop jar /opt/app/processing-java-1.0.0.jar com.ynov.pipeline.ExportToHdfs


Sortie attendue :

[ExportToHdfs] Exported rows = 5000

📌 Lancer le Job MapReduce

Toujours dans le NameNode :

hadoop jar /opt/app/processing-java-1.0.0.jar com.ynov.pipeline.GenreTopTracksJob


Résultats dans HDFS :

hdfs dfs -ls /data/phase2/output/

📌 Consulter dans Hive
docker exec -it hive-server bash
beeline -u jdbc:hive2://localhost:10000/default


Créer table d’entrée :

CREATE EXTERNAL TABLE genre_tracks (
  genre_id INT,
  genre_name STRING,
  track_id INT,
  title STRING,
  artist_name STRING,
  rank INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/phase2/input/';

🛠️ Technologies utilisées

Kafka

Python (Producer + Consumer)

PostgreSQL

Hadoop (HDFS, YARN, MapReduce)

Java 11 — Maven

Hive

Docker & docker-compose

👤 Auteurs

Abdelhamid Belhadj Kacem

Stéphanie

Assaa
Projet réalisé dans le cadre du Mastère Data Engineering – Ynov Campus Lyon