#!/bin/bash

docker rm -f hadoop-zookeeper &> /dev/null
docker run -itd \
      -p 20022:22 \
      -p 2181:2181 \
      -p 2888:2888 \
      -p 3888:3888 \
      --name hadoop-zookeeper  \
      -v /var/lib/zookeeper:/var/lib/zookeeper \
      -v /var/log/zookeeper:/var/log/zookeeper  \
      thushear/zookeeper:2.0 zk1,zk2,zk3 $ID   &> /dev/null