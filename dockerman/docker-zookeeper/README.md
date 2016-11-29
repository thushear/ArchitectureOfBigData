# Intro

构建有ssh的zookeeper单机节点和分布式节点

- docker image with zookeeper

- sudo docker build -t thushear/zookeeper:5.0 .

- sudo docker run -itd  -p 62022:22 \
    --name hadoop-zookeeper \
     --hostname hadoop-zookeeper \
    thushear/zookeeper:2.0 &> /dev/null

## 单节点启动Zookeeper
- sudo docker run -itd \
        -p 20022:22 \
        -p 2181:2181 \
        -p 2888:2888 \
        -p 3888:3888 \
        --name hadoop-zookeeper1  \
        --hostname hadoop-zookeeper1 \
        -v /var/lib/zookeeper:/var/lib/zookeeper \
        -v /var/log/zookeeper:/var/log/zookeeper  \
        thushear/zookeeper:5.0 hadoop-zookeeper1 1 

## 环境变量传参
- sudo docker run -itd \
        -p 20022:22 \
        -p 2181:2181 \
        -p 2888:2888 \
        -p 3888:3888 \
        -e "ZOOKEEPER_ID=1" \
        -e "ZOOKEEPER_SERVERS=hadoop-zookeeper1"  \
        --name hadoop-zookeeper1  \
        --hostname hadoop-zookeeper1 \
        -v /var/lib/zookeeper:/var/lib/zookeeper \
        -v /var/log/zookeeper:/var/log/zookeeper  \
        thushear/zookeeper:5.0

## 通过docker-compose启动集群
- sudo docker-compose up
查看zookeeper角色
./zkServer.sh status
