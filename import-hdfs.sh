#!/bin/sh
hdfs dfs -put $1 /tmp
sudo -u hdfs hdfs dfs -chown hive:hive /tmp/$1
