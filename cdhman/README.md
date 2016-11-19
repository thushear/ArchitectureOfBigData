## CDH Hadoop 分布式集群搭建

### 创建自定义镜像
```
sudo docker build -t cdh/hadoop:4.0 .
```
### 1. create hadoop network

```
sudo docker network create --driver=bridge hadoop
```

### 2. start container

```
sudo ./start-container.sh
```

**output:**

```
start hadoop-master container...
start hadoop-slave1 container...
start hadoop-slave2 container...
root@hadoop-master:~# 
```
- start 3 containers with 1 master and 2 slaves
- you will get into the /root directory of hadoop-master container

#####5. start hadoop

```
./start-hadoop.sh
```

#####6. run wordcount

```
./run-wordcount.sh
```

**output**

```
input file1.txt:
Hello Hadoop

input file2.txt:
Hello Docker

wordcount output:
Docker    1
Hadoop    1
Hello    2
```

### 安装Mysql 
- sudo apt-get install mysql-server mysql-client
- sudo service mysql start
- sudo service mysql status





### 添加特性
- 因为兼容性问题升级CDH5  
- 因为网速原因把hadoop压缩包下载到本地 COPY 到容器中
- 暴漏22端口方便远程ssh连接
- 添加日志聚集配置 
- 启动jobserver
- 添加root密码
- 采用版本
- hadoop-2.5.0-cdh5.3.6.tar.gz
- hive-0.13.1-cdh5.3.6.tar.gz

-  yarn jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0-cdh5.3.6.jar wordcount input output
