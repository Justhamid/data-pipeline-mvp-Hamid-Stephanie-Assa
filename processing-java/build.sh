#!/bin/bash
set -e

HADOOP_HOME=/opt/hadoop
JAR_NAME=MapReduceJobs.jar

CLASSPATH=$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\n' ':')

mkdir -p classes

# Compile TOUT en une seule commande, en respectant les packages
javac -cp "$CLASSPATH" -sourcepath . -d classes $(find . -name "*.java")

cd classes
jar cf ../$JAR_NAME *
cd ..

echo "✅ Compilation terminée → JAR créé : $JAR_NAME"
