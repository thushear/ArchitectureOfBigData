#!/bin/bash

sudo docker rm -f hadoop-zookeeper1 &> /dev/null
sudo docker run -itd \
      -p 20022:22 \
      -p 2181:2181 \
      -p 2888:2888 \
      -p 3888:3888 \
      --name hadoop-zookeeper1  \
      --hostname hadoop-zookeeper1 \
      -v /var/lib/zookeeper:/var/lib/zookeeper \
      -v /var/log/zookeeper:/var/log/zookeeper  \
      thushear/zookeeper:5.0 hadoop-zookeeper1,zk2,zk3 1   &> /dev/null
