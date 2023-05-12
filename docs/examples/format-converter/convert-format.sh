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

LIBS_DIR="app-libs"
mkdir -p $LIBS_DIR
cd $LIBS_DIR

SPARK_TOOLS_ARTIFACT="spark-tools_2.12"
SPARK_TOOLS_VERSION="1.0.0-SNAPSHOT"
SPARK_TOOLS_JAR="$SPARK_TOOLS_ARTIFACT-$SPARK_TOOLS_VERSION.jar"

SPARK_UTILS_VERSION="1.0.0-RC4"
SPARK_UTILS_CORE_ARTIFACT="spark-utils-core_2.12"
SPARK_UTILS_CORE_JAR="$SPARK_UTILS_CORE_ARTIFACT_ARTIFACT-$SPARK_UTILS_VERSION.jar"
SPARK_UTILS_IO_ARTIFACT="spark-utils-io_2.12"
SPARK_UTILS_IO_JAR="$SPARK_UTILS_IO_ARTIFACT_ARTIFACT-$SPARK_UTILS_VERSION.jar"
SPARK_UTILS_PURECONFIG_ARTIFACT="spark-utils-io-pureconfig_2.12"
SPARK_UTILS_PURECONFIG_JAR="$SPARK_UTILS_PURECONFIG_ARTIFACT_ARTIFACT-$SPARK_UTILS_VERSION.jar"

SCALA_UTILS_ARTIFACT="scala-utils-core_2.12"
SCALA_UTILS_VERSION="1.1.2"
SCALA_UTILS_JAR="$SCALA_UTILS_ARTIFACT-$SCALA_UTILS_VERSION.jar"

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
bring_tupol_artifact $SPARK_UTILS_VERSION $SPARK_UTILS_CORE_ARTIFACT
bring_tupol_artifact $SPARK_UTILS_VERSION $SPARK_UTILS_IO_ARTIFACT
bring_tupol_artifact $SPARK_UTILS_VERSION $SPARK_UTILS_PURECONFIG_ARTIFACT
bring_tupol_artifact $SCALA_UTILS_VERSION $SCALA_UTILS_ARTIFACT

TYPESAFE_CONFIG_JAR="config-1.4.2.jar"
if [ ! -f $TYPESAFE_CONFIG_JAR ]; then
  URL="https://repo1.maven.org/maven2/com/typesafe/config/1.4.2/$TYPESAFE_CONFIG_JAR"
  echo "$TYPESAFE_CONFIG_JAR was not found locally; bringing a version from $URL"
  wget "$URL"
fi

PURECONFIG_JAR="pureconfig_2.12-0.17.4.jar"
if [ ! -f $PURECONFIG_JAR ]; then
  URL="https://repo1.maven.org/maven2/com/github/pureconfig/pureconfig_2.12/0.17.4/$PURECONFIG_JAR"
  echo "$PURECONFIG_JAR was not found locally; bringing a version from $URL"
  wget "$URL"
fi

DELTA_JAR="delta-core_2.12-2.1.1.jar"
if [ ! -f $DELTA_JAR ]; then
  URL="https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.1.1/$DELTA_JAR"
  echo "$DELTA_JAR was not found locally; bringing a version from $URL"
  wget "$URL"
fi

cd ../

JARS=$JARS",$LIBS_DIR/$SCALA_UTILS_JAR,$LIBS_DIR/$SPARK_UTILS_JAR"
JARS=$JARS",$LIBS_DIR/$TYPESAFE_CONFIG_JAR,$LIBS_DIR/$PURECONFIG_JAR"
JARS=$JARS",$LIBS_DIR/$DELTA_JAR"
JARS=""; for f in `ls ./$LIBS_DIR`; do JARS+=/$LIBS_DIR/$f,; done;

###############################################################################
## Spark Submit Section                                                      ##
###############################################################################

docker run --rm -it \
-v "$SCRIPT_DIR/resources:/resources" \
-v "$SCRIPT_DIR/$LIBS_DIR:/$LIBS_DIR" \
-v "$APPLICATION_CONF:/$LIBS_DIR/application.conf" \
apache/spark:v3.1.3 \
/opt/spark/bin/spark-submit -v \
--deploy-mode client \
--master "local[*]" \
--name $APP_NAME \
--class $MAIN_CLASS \
--conf spark.yarn.submit.waitAppCompletion=true \
--queue default \
--jars "$JARS" \
--files "/$LIBS_DIR/application.conf" \
--driver-java-options "-Dconfig.file=/$LIBS_DIR/application.conf" \
/$LIBS_DIR/$SPARK_TOOLS_JAR \
--conf
