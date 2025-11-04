# Dockerfile pour MapReduce
FROM eclipse-temurin:11-jdk

# Définir le répertoire de travail
WORKDIR /app

# Installer les dépendances nécessaires
RUN apt-get update && \
    apt-get install -y maven wget curl vim ssh rsync && \
    rm -rf /var/lib/apt/lists/*

# Télécharger et décompresser Hadoop 3.3.4
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    rm /tmp/hadoop.tar.gz && \
    mv /opt/hadoop-3.3.6 /opt/hadoop

# Définir les variables d'environnement Hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Copier le code MapReduce depuis le projet local
COPY mapreduce /app/mapreduce

# Exposer éventuellement un port pour Hadoop si besoin
# EXPOSE 9000 9870

# Copier le script build.sh
COPY build.sh /app/mapreduce/build.sh
RUN chmod +x /app/mapreduce/build.sh

# Commande par défaut (peut être remplacée pour lancer un job Hadoop)
CMD ["tail", "-f", "/dev/null"]
