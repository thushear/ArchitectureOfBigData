# Docker Java ssh 
- openjdk 7 java with ssh base docker image

- docker build -t thushear/java:1.0 .

- docker run -itd  -p 10022:22 \
    --name ssh-java \
    thushear/java:1.0 &> /dev/null

- docker run -itd  -p 10022:22 \
      --name hadoop-java \
      --hostname hadoop-java \
      thushear/java:1.0 &> /dev/null
