# File hadoop-env.sh - Thiet lap cac bien moi truong cho dich vu Hadoop
# Tren base image eclipse-temurin:8-jdk-jammy thi JAVA_HOME mac dinh nam o /opt/java/openjdk

export JAVA_HOME=/opt/java/openjdk
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs
export HADOOP_PID_DIR=/tmp

# Thiet lap bo nho JVM Heap (neu may thieu RAM thi co the ha muc max xuong con 256m)
export HADOOP_HEAPSIZE_MAX=512m
export HADOOP_HEAPSIZE_MIN=256m

# Chay cac dich vu bang user root trong Docker (phu hop de test hoac lam demo)
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root