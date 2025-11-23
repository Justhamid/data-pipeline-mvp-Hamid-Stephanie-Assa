# Image Python légère
FROM python:3.11-slim

# Dossier de travail dans le conteneur
WORKDIR /app

# Copie le fichier requirements.txt
COPY requirements.txt .

# Installe les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copie le code source
COPY producer/ ./producer/
COPY consumer/ ./consumer/

# Pas de CMD ici : chaque service (producer, consumer) le définit dans docker-compose
