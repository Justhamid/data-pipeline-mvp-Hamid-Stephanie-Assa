#!/bin/bash
# build.sh : compile les jobs MapReduce et crée un JAR

set -e

HADOOP_HOME=/opt/hadoop
SRC_DIR=./jobs
MAPPER_DIR=./mapper
REDUCER_DIR=./reducer
JAR_NAME=MapReduceJobs.jar

# Construire le classpath Hadoop
CLASSPATH=$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\n' ':')

# Créer le dossier pour les classes compilées
mkdir -p classes

# Compiler les fichiers .java
javac -cp "$CLASSPATH" -d classes $SRC_DIR/*.java $MAPPER_DIR/*.java $REDUCER_DIR/*.java

# Créer le jar
cd classes
jar cf ../$JAR_NAME *
cd ..

echo "Compilation terminée. JAR créé : $JAR_NAME"
