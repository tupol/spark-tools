#!/bin/bash

echo "###############################################################################"
echo "##                                                                           ##"
echo "## SQL Processor Script                                                      ##"
echo "##                                                                           ##"
echo "###############################################################################"


###############################################################################
## Application Configuration Setup                                           ##
###############################################################################

USER_APPLICATION_CONF="application.conf"
if [ ! -z $1 ]; then
  USER_APPLICATION_CONF="$1"
fi

if [ ! -f $USER_APPLICATION_CONF ]; then
 echo "The configuration file $USER_APPLICATION_CONF does not exist or it is not accessible."
 exit -1
fi

APPLICATION_CONF_DIR="tmp"
APPLICATION_CONF="$APPLICATION_CONF_DIR/application.conf"
rm -rf $APPLICATION_CONF_DIR
mkdir $APPLICATION_CONF_DIR
cp -f $USER_APPLICATION_CONF $APPLICATION_CONF

###############################################################################
## Application & Dependencies Setup                                          ##
###############################################################################

LIBS_DIR="libs"
mkdir -p $LIBS_DIR
cd $LIBS_DIR

SPARK_TOOLS_ARTIFACT="spark-tools_2.11"
SPARK_TOOLS_VERSION="0.3.0-SNAPSHOT"
SPARK_TOOLS_JAR="$SPARK_TOOLS_ARTIFACT-$SPARK_TOOLS_VERSION.jar"

SPARK_UTILS_ARTIFACT="spark-utils_2.11"
SPARK_UTILS_VERSION="0.3.1"
SPARK_UTILS_JAR="$SPARK_UTILS_ARTIFACT-$SPARK_UTILS_VERSION.jar"

SCALA_UTILS_ARTIFACT="scala-utils_2.11"
SCALA_UTILS_VERSION="0.2.0"
SCALA_UTILS_JAR="$SCALA_UTILS_ARTIFACT-$SCALA_UTILS_VERSION.jar"

function bring_tupol_artifact {

  VERSION="$1"
  ARTIFACT="$2"
  JAR="$ARTIFACT-$VERSION.jar"

  if [[ $VERSION == *"SNAPSHOT" ]]; then
    REPO_PATH="https://oss.sonatype.org/content/repositories/snapshots/org/tupol"
  else
    REPO_PATH="http://central.maven.org/maven2/org/tupol"
  fi
  URL="$REPO_PATH/$ARTIFACT/$VERSION/$JAR"

  if [ ! -f $JAR ]; then
    echo "$JAR was not found locally; bringing a version from $URL"
    wget "$URL"
  fi
}

bring_tupol_artifact $SPARK_TOOLS_VERSION $SPARK_TOOLS_ARTIFACT
bring_tupol_artifact $SPARK_UTILS_VERSION $SPARK_UTILS_ARTIFACT
bring_tupol_artifact $SCALA_UTILS_VERSION $SCALA_UTILS_ARTIFACT

TYPESAFE_CONFIG_JAR="config-1.3.0.jar"
if [ ! -f $TYPESAFE_CONFIG_JAR ]; then
  URL="http://central.maven.org/maven2/com/typesafe/config/1.3.0/$TYPESAFE_CONFIG_JAR"
  echo "$TYPESAFE_CONFIG_JAR was not found locally; bringing a version from $URL"
  wget "$URL"
fi

SCALAZ_JAR="scalaz-core_2.11-7.2.26.jar"
if [ ! -f $SCALAZ_JAR ]; then
  URL="http://central.maven.org/maven2/org/scalaz/scalaz-core_2.11/7.2.26/$SCALAZ_JAR"
  echo "$SCALAZ_JAR was not found locally; bringing a version from $URL"
  wget "$URL"
fi

cd ../

JARS="$LIBS_DIR/$TYPESAFE_CONFIG_JAR,$LIBS_DIR/$SCALAZ_JAR,$LIBS_DIR/$SCALA_UTILS_JAR,$LIBS_DIR/$SPARK_UTILS_JAR"


###############################################################################
## Spark Submit Section                                                      ##
###############################################################################

spark-submit  -v  \
--master local[*] \
--deploy-mode client \
--class org.tupol.spark.tools.SimpleSqlProcessor \
--name SqlProcessor \
--conf spark.yarn.submit.waitAppCompletion=true \
--queue default \
--files "$APPLICATION_CONF" \
--jars "$JARS" \
$LIBS_DIR/$SPARK_TOOLS_JAR \
my-filter-id="1001"

