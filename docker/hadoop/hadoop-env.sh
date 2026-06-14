# ============================================================
#  hadoop-env.sh — Environment variables for Hadoop daemons
#  eclipse-temurin:8-jdk-jammy has JAVA_HOME at /opt/java/openjdk
# ============================================================

export JAVA_HOME=/opt/java/openjdk
export HADOOP_HOME=/opt/hadoop
# FIX: KHÔNG hardcode HADOOP_CONF_DIR ở đây.
# hadoop-env.sh được source bởi mỗi hdfs/yarn command → nếu hardcode thì
# lệnh `export HADOOP_CONF_DIR=/hadoop/conf-override` trong entrypoint.sh
# sẽ bị RESET về default mỗi lần hdfs/yarn chạy, khiến DataNode đọc
# config gốc thay vì conf-override (không có port offset, không có hostname).
# Dùng ${HADOOP_CONF_DIR:-...} để chỉ set khi chưa có giá trị từ ngoài.
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs
export HADOOP_PID_DIR=/tmp

# Heap sizes (adjust down to 256m if RAM is tight)
export HADOOP_HEAPSIZE_MAX=512m
export HADOOP_HEAPSIZE_MIN=256m

# Run as root inside Docker (acceptable for dev/demo environments)
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root