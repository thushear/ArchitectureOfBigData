#!/bin/bash

echo  "\n"
docker run -itd  -p 10022:22 \
    --name ssh-java \
    --hostname hadoop-java \
    thushear/java:1.0 &> /dev/null
docker exec -it hadoop-java bash
echo "\n success ! \n"
