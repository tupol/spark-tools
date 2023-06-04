#!/bin/bash

echo "###############################################################################"
echo "##                                                                           ##"
echo "## Format Converter SAMPLE Script                                            ##"
echo "##                                                                           ##"
echo "###############################################################################"

SCRIPT_DIR="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

###############################################################################
## Application Configuration Setup                                           ##
###############################################################################

APP_NAME=FormatConverter
MAIN_CLASS=org.tupol.spark.tools.FormatConverter

USER_APPLICATION_CONF="$1"

if [ ! -f $USER_APPLICATION_CONF ]; then
 echo "The configuration file $USER_APPLICATION_CONF does not exist or it is not accessible."
 exit -1
fi

APPLICATION_CONF="$(pwd)/$USER_APPLICATION_CONF"

###############################################################################
## Application & Dependencies Setup                                          ##
###############################################################################

OUT_DIR="out"
mkdir -p $OUT_DIR

APP_DIR="app"
mkdir -p $APP_DIR
cd $APP_DIR

cp $APPLICATION_CONF ./application.conf

SPARK_TOOLS_ARTIFACT="spark-tools_2.12"
SPARK_TOOLS_VERSION="assembly"
SPARK_TOOLS_JAR="$SPARK_TOOLS_ARTIFACT-$SPARK_TOOLS_VERSION.jar"

function bring_tupol_artifact {

  VERSION="$1"
  ARTIFACT="$2"
  JAR="$ARTIFACT-$VERSION.jar"

  if [[ $VERSION == *"SNAPSHOT" ]]; then
    REPO_PATH="https://oss.sonatype.org/content/repositories/snapshots/org/tupol"
  else
    REPO_PATH="https://repo1.maven.org/maven2/org/tupol"
  fi
  URL="$REPO_PATH/$ARTIFACT/$VERSION/$JAR"

  if [ ! -f $JAR ]; then
    echo "$JAR was not found locally; bringing a version from $URL"
    wget "$URL"
  fi
}

bring_tupol_artifact $SPARK_TOOLS_VERSION $SPARK_TOOLS_ARTIFACT

cd ../

###############################################################################
## Spark Submit Section                                                      ##
###############################################################################

docker run --rm -it \
-v "$SCRIPT_DIR/resources:/resources" \
-v "$SCRIPT_DIR/$APP_DIR:/$APP_DIR" \
-v "$SCRIPT_DIR/$OUT_DIR:/$OUT_DIR" \
apache/spark:v3.1.3 \
/opt/spark/bin/spark-submit -v \
--deploy-mode client \
--master "local[*]" \
--name $APP_NAME \
--class $MAIN_CLASS \
--conf spark.yarn.submit.waitAppCompletion=true \
--queue default \
--files "/$APP_DIR/application.conf" \
--driver-java-options "-Dconfig.file=/$APP_DIR/application.conf" \
/$APP_DIR/$SPARK_TOOLS_JAR
