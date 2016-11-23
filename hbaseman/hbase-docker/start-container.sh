#!/bin/bash

# the default node number is 3
N=${1:-3}


# start hadoop master container
sudo docker rm -f hadoop-master &> /dev/null
echo "start hadoop-master container..."
sudo docker run -itd \
                --net=hadoop \
                -p 50070:50070 \
                -p 8088:8088 \
                -p 11000:11000 \
                -p 30022:22 \
                -p 9000:9000 \
                -p 19888:19888 \
                -p 8000:8000 \
                -p 60010:60010 \
                -p 60020:60020 \
                -p 60030:60030 \
                -p 2181:2181 \
                --name hadoop-master \
                --hostname hadoop-master \
                cdh/hadoop:6.0 &> /dev/null


# start hadoop slave container
i=1
while [ $i -lt $N ]
do
	sudo docker rm -f hadoop-slave$i &> /dev/null
	echo "start hadoop-slave$i container..."
	sudo docker run -itd \
	                --net=hadoop \
	                -P    \
	                --name hadoop-slave$i \
	                --hostname hadoop-slave$i \
	                cdh/hadoop:6.0 &> /dev/null
	i=$(( $i + 1 ))
done

# get into hadoop master container
sudo docker exec -it hadoop-master bash
