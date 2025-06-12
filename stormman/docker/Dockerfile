FROM baqend/storm:1.0.0
LABEL maintainer="thushear <lucas421634258@gmail.com>"

RUN sed -e 's|http://archive.ubuntu.com/ubuntu/|http://mirrors.163.com/ubuntu/|g' /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends openssh-server \
    && rm -rf /var/lib/apt/lists/*



# Install and set everything up

# ssh without key
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

COPY ssh_config /root/.ssh/config

RUN mkdir /var/run/sshd \
    && echo 'root:root' | chpasswd \
    && sed -ri 's/^PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config
EXPOSE 22
CMD    ["/usr/sbin/sshd", "-D"]

