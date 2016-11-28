# Intro
- docker image with zookeeper

docker run -itd  -p 10022:22 \
    --name hadoop-zookeeper \
    thushear/zookeeper:1.0 &> /dev/null