#!/bin/bash
#############################################################
# DEMO
# This is a demo script and it must be ran from the project directory !

# FIXED PARAMETERS; DO NOT CHANGE!
SCRIPT_DIR="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
JAR_NAME=openflights-assembly-0.1.0-SNAPSHOT.jar
SOURCE_JAR=$SCRIPT_DIR/target/scala-2.12/$JAR_NAME
TARGET_JAR=/app/$JAR_NAME
MAIN_CLASS=org.tupol.fun.openflights.TopAirportsBatch

# ######################################################################
# CONFIGURATION PARAMETERS
WORKING_DIR="/tmp/test-data"
INPUT_DATA=$SCRIPT_DIR/data/routes.dat

# ######################################################################
# Script setup and run

rm -rf $WORKING_DIR
mkdir -p $WORKING_DIR/input
cp $INPUT_DATA $WORKING_DIR/input/input.dat

docker run --rm -it \
-v "$SOURCE_JAR:$TARGET_JAR" \
-v "$WORKING_DIR:/test-data" \
apache/spark:v3.1.3 /opt/spark/bin/spark-submit \
--deploy-mode client \
--master "local[*]" \
--class $MAIN_CLASS \
$TARGET_JAR \
input.path=/test-data/input/input.dat \
output.path=/test-data/output

echo "--------------------------------------------------"
cat $WORKING_DIR/output/part*.*