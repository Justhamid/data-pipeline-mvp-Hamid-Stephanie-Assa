#!/bin/sh
# Script de compilation pour les jobs MapReduce, compatible /bin/sh

# L'équivalent de 'set -e' pour sh. Arrête le script si une commande échoue.
# Nous ne pouvons pas utiliser 'set -e' car l'interpréteur par défaut ne le supporte pas toujours.
# Chaque commande javac, cd, jar sera suivie d'une vérification.

# --- CHEMINS DANS LE CONTENEUR ---
HADOOP_HOME="/opt/hadoop"
HADOOP_COMMON_LIB_DIR="$HADOOP_HOME/share/hadoop/common/lib"

# Dossiers de sources (relatifs à /app)
SRC_ROOT="mapreduce" # <-- Le dossier racine de tous les packages (jobs, mapper, reducer)

# Dossiers et noms de fichiers de destination
BUILD_DIR="classes"
JAR_NAME="music-data-pipeline.jar"

# --- 1. NETTOYAGE ET CLASSPATH ---
echo "--- Nettoyage et Préparation ---"
rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR

# Création du Classpath de compilation : tous les JARs nécessaires
# Les commandes find sont plus robustes si on utilise 'sh'
CLASSPATH=$(find $HADOOP_HOME/share/hadoop/common/ -name "*.jar" | tr '\n' ':')
CLASSPATH="$CLASSPATH:$(find $HADOOP_HOME/share/hadoop/mapreduce/ -name "*.jar" | tr '\n' ':')"
# Ajout du driver JDBC
CLASSPATH="$CLASSPATH:$HADOOP_COMMON_LIB_DIR/postgresql-jdbc.jar"


# --- 2. COMPILATION ---
echo "--- Compilation des classes Java ---"

# Utilisation de find pour récupérer la liste des fichiers .java
# Recherche dans le répertoire parent 'mapreduce'
JAVA_SOURCES="$(find $SRC_ROOT -name "*.java")"

# CORRECTION CRUCIALE : 
# 1. Utilisation de $SRC_ROOT (mapreduce) comme sourcepath pour résoudre les packages.
# 2. Simplification des arguments pour éviter les erreurs de chemin.
# 3. Le chemin de compilation des sources doit être DANS mapreduce
javac -cp "$CLASSPATH" -sourcepath $SRC_ROOT $JAVA_SOURCES -d $BUILD_DIR

if [ $? -ne 0 ]; then
    echo "❌ Échec de la compilation. Code de sortie $?."
    exit 1
fi
echo "✅ Compilation réussie. Classes stockées dans $BUILD_DIR"

# --- 3. CRÉATION DU JAR ---
echo "--- Création du fichier JAR exécutable ---"

# Création du JAR contenant toutes les classes compilées
cd $BUILD_DIR
if [ $? -ne 0 ]; then exit 1; fi # Vérification après cd

# Création du JAR
jar -cvf ../$JAR_NAME ./*
if [ $? -ne 0 ]; then exit 1; fi # Vérification après jar

cd ..

echo "✅ Fichier JAR créé : $JAR_NAME dans le répertoire courant."
echo "Le job est prêt à être exécuté."
