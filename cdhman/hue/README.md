# Hue自定义镜像创建

- sudo docker build  -t thushear/hue:1.0 .

- sudo docker rm -f hadoop-hue &> /dev/null
- sudo docker run -itd --net=hadoop  -p 8888:8888 -p 40022:22    --name hadoop-hue --hostname hadoop-hue thushear/hue:1.0 &> /dev/null

- sudo docker exec -it  hadoop-hue bash

- 启动Hue
- ./build/env/bin/hue runserver_plus 0.0.0.0:8888
