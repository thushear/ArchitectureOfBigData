# Docker-Storm

# 镜像构建
- sudo docker build -t thushear/storm:1.0 .

# 单机环境运行
- sudo docker run -itd -p 30022:22 --net=test-storm -e "STORM_ZOOKEEPER_SERVERS=hadoop-zookeeper1,hadoop-zookeeper2,hadoop-zookeeper3"  \
          --name hadoop-storm  \
          --hostname hadoop-storm \
          thushear/storm:1.0 nimbus -c nimbus.host=nimbus
