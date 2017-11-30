#!/bin/sh

home=$(cd `dirname $0`; cd ..; pwd)
bin_home=$home/bin
conf_home=$home/conf
logs_home=$home/logs
data_home=$home/data
lib_home=$home/lib

#服务器配置文件
configFile=${conf_home}/my.properties

spark_submit=/home/hadoop/app/spark/bin/spark-submit
