#!/bin/sh

home=$(cd `dirname $0`; cd ..; pwd)

. ${home}/bin/common.sh

hdfs dfs -mkdir -p /spark-streaming/behavior/stop

