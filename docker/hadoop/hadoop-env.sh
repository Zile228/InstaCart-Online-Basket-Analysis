# ============================================================
#  hadoop-env.sh - Biến môi trường cho các tiến trình Hadoop
#  eclipse-temurin:8-jdk-jammy cấu hình JAVA_HOME tại /opt/java/openjdk
# ============================================================

export JAVA_HOME=/opt/java/openjdk
export HADOOP_HOME=/opt/hadoop

# Tránh ghi đè cứng HADOOP_CONF_DIR để hệ thống nhận thư mục cấu hình động từ entrypoint.sh khi cần thiết
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs
export HADOOP_PID_DIR=/tmp

# Cấu hình bộ nhớ Heap (có thể giảm xuống 256m nếu máy yếu hoặc thiếu RAM)
export HADOOP_HEAPSIZE_MAX=512m
export HADOOP_HEAPSIZE_MIN=256m

# Chạy dưới quyền root trong Docker (phù hợp cho môi trường thử nghiệm và phát triển)
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root