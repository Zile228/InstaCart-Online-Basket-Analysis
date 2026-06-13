# Hadoop 3.4.3 - Image dung chung cho ca NameNode va DataNode
# Base image bat buoc phai dung JDK 8 de tuong thich voi Hadoop
# Node chay vai tro nao se do bien moi truong HADOOP_ROLE quyet dinh khi start container

FROM eclipse-temurin:8-jdk-jammy

# Khai bao cac bien moi truong can thiet cho he thong va Hadoop
ENV HADOOP_VERSION=3.4.3
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# Cai dat cac thu vien, cong cu he thong co ban
RUN apt-get update && apt-get install -y \
    wget \
    ssh \
    rsync \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Cau hinh SSH key de cac node Hadoop co the tuong tac qua lai khong can password
RUN ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
    && chmod 0600 /root/.ssh/authorized_keys \
    && echo "Host *\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile /dev/null" > /root/.ssh/config

# Tai va giai nen Hadoop vao thu muc /opt
RUN wget --progress=bar:force:noscroll \
    https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Chuan bi sao chep cac file cau hinh
# Vi Hadoop khong tu doc duoc bien moi truong tu file XML nen ta dung bien 
# build-arg DFS_REPLICATION (mac dinh la 1) roi dung sed thay vao file config luc build.
ARG DFS_REPLICATION=1
COPY core-site.xml     ${HADOOP_HOME}/etc/hadoop/core-site.xml
COPY hdfs-site.xml     ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml

# Dung sed de tim den the dfs.replication va cap nhat lai dung gia tri mong muon
RUN sed -i "/<name>dfs.replication<\/name>/{n; s|<value>[0-9]*</value>|<value>${DFS_REPLICATION}</value>|}" \
    ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml

COPY yarn-site.xml     ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
COPY mapred-site.xml   ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
COPY hadoop-env.sh     ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

# Cau hinh file entrypoint de khoi dong cac dich vu khi chay container
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Tao cac thu muc can thiet de lu tru data va log cua Hadoop
RUN mkdir -p /hadoop/dfs/name \
             /hadoop/dfs/data \
             /hadoop/tmp \
             ${HADOOP_HOME}/logs

# Mo cac port dung de ket noi va quan ly:
# - NameNode: 9870 (giao dien Web UI), 9000 (giao tiep noi bo RPC)
# - DataNode: 9864 (Web UI), 9866 (truyen nhan data), 9867 (IPC)
# - YARN: 8088 (Web UI ResourceManager), 8042 (Web UI NodeManager)
EXPOSE 9870 9000 9864 9866 9867 8088 8042

ENTRYPOINT ["/entrypoint.sh"]