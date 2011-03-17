#!/usr/bin/env bash
COMMON_HOME=/home/hadoop/common
CLASSPATH=$COMMON_HOME/gruter-common-0.9.0-core.jar
for f in $COMMON_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in $COMMON_HOME/lib/jetty-ext/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

java -classpath "$CLASSPATH" com.gruter.common.zk.ZKManagerWebServer "$@"