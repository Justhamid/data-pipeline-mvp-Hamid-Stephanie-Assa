# Dockerfile pour MapReduce
FROM eclipse-temurin:11-jdk

# Définir le répertoire de travail
WORKDIR /app

# Installer les dépendances nécessaires
RUN apt-get update && \
    # AJOUT DE BASH ici pour garantir que le shebang #!/bin/bash fonctionne
    apt-get install -y maven wget curl vim ssh rsync bash && \
    rm -rf /var/lib/apt/lists/*

# --- CONFIGURATION HADOOP CLIENT ---
# Télécharger et décompresser Hadoop 3.3.6 (Client seulement pour l'exécution du job)
ENV HADOOP_VERSION 3.3.6
ENV HADOOP_HOME /opt/hadoop

RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm /tmp/hadoop.tar.gz

# --- AJOUT DU DRIVER JDBC POSTGRESQL ---
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -O $HADOOP_HOME/share/hadoop/common/lib/postgresql-jdbc.jar

# Définir les variables d'environnement Hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_HOME/share/hadoop/common/lib/postgresql-jdbc.jar

# CORRECTION CRUCIALE : Copier le dossier mapreduce et le script build.sh à la racine de WORKDIR (/app)
# L'instruction COPY dans Dockerfile ne prend pas en compte les volumes
COPY mapreduce /app/mapreduce

# CORRECTION CRUCIALE : Le build.sh doit être à la racine /app, à côté du dossier mapreduce
COPY build.sh /app/build.sh 
RUN chmod +x /app/build.sh

# Commande par défaut pour maintenir le conteneur actif
CMD ["tail", "-f", "/dev/null"]
