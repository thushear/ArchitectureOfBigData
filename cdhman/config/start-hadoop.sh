#!/bin/bash

echo -e "\n"

$HADOOP_HOME/bin/hdfs namenode -format

echo -e "\n"

$HADOOP_HOME/sbin/start-dfs.sh

echo -e "\n"

$HADOOP_HOME/sbin/start-yarn.sh

echo -e "\n"

$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

echo -e "\n"
