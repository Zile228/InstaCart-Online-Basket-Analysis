# ============================================================
#  Hadoop 3.4.3 — Unified Image (NameNode + DataNode)
#  Base: eclipse-temurin:8-jdk-jammy (JDK 8 bắt buộc cho Hadoop)
#  Vai trò được xác định qua biến môi trường: HADOOP_ROLE
# ============================================================

FROM eclipse-temurin:8-jdk-jammy

# ── Environment ──────────────────────────────────────────────
ENV HADOOP_VERSION=3.4.3
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# ── System packages ──────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    wget \
    ssh \
    rsync \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# ── SSH setup (Hadoop cần SSH giữa các node) ─────────────────
RUN ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
    && chmod 0600 /root/.ssh/authorized_keys \
    && echo "Host *\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile /dev/null" > /root/.ssh/config

# ── Download and install Hadoop 3.4.3 ────────────────────────
RUN wget --progress=bar:force:noscroll \
    https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# ── Copy Hadoop config files ──────────────────────────────────
# DFS_REPLICATION: truyền qua --build-arg hoặc args: trong docker-compose
# Hadoop không đọc env var trong XML → phải inject tại build time bằng sed
# docker-compose.master.yml: args: DFS_REPLICATION: "${DFS_REPLICATION:-1}"
ARG DFS_REPLICATION=1
COPY core-site.xml     ${HADOOP_HOME}/etc/hadoop/core-site.xml
COPY hdfs-site.xml     ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
# Chỉ thay value trong đúng block dfs.replication, không đụng các property khác
# dùng address-range: từ dòng chứa dfs.replication đến </property> gần nhất,
# trong đó tìm <value>N</value> và thay bằng DFS_REPLICATION.
RUN sed -i "/<name>dfs.replication<\/name>/{n; s|<value>[0-9]*</value>|<value>${DFS_REPLICATION}</value>|}" \
    ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
COPY yarn-site.xml     ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
COPY mapred-site.xml   ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
COPY hadoop-env.sh     ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

# ── Entrypoint script ─────────────────────────────────────────
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ── Data directories ─────────────────────────────────────────
RUN mkdir -p /hadoop/dfs/name \
             /hadoop/dfs/data \
             /hadoop/tmp \
             ${HADOOP_HOME}/logs

# ── Exposed ports ─────────────────────────────────────────────
#  NameNode: 9870 (Web UI), 9000 (RPC)
#  DataNode: 9864 (Web UI), 9866 (data transfer), 9867 (IPC)
#  YARN:     8088 (ResourceManager UI), 8042 (NodeManager UI)
EXPOSE 9870 9000 9864 9866 9867 8088 8042

ENTRYPOINT ["/entrypoint.sh"]
