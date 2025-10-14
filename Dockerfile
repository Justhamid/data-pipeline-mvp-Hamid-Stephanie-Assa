# Image de base Python légère
FROM python:3.10-slim

# Définit le répertoire de travail dans le conteneur
WORKDIR /app

# Copie les répertoires producer et consumer dans le conteneur
COPY ./producer /app/producer
COPY ./consumer /app/consumer

# Installe les dépendances nécessaires pour Kafka, PostgreSQL et variables d'environnement
RUN pip install kafka-python psycopg2-binary python-dotenv

# Garde le conteneur actif (à modifier plus tard quand on exécutera ton producer/consumer)
CMD ["tail", "-f", "/dev/null"]
