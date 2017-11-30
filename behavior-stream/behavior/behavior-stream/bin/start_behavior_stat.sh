#!/bin/sh

deleteCheckpoint=$1

home=$(cd `dirname $0`; cd ..; pwd)

. ${home}/bin/common.sh

hdfs dfs -rm -r /spark-streaming/behavior/stop

if [ "$deleteCheckpoint" == "delck" ]; then
    hdfs dfs -rm -r /spark-streaming/checkpoint/behavior
fi

${spark_submit} \
--master yarn \
--deploy-mode cluster \
--name behavior \
--driver-memory 512M \
--executor-memory 512M \
--class com.djt.spark.streaming.UserBehaviorStreaming \
${lib_home}/behavior-stream-jar-with-dependencies.jar ${configFile} \
>> ${logs_home}/behavior.log 2>&1 &

echo $! > ${logs_home}/behavior_stream.pid
