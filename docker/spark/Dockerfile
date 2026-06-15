# ============================================================
#  Spark 4.1.1 - Custom Image
#  Cấu hình: Spark 4.1.1 + Scala 2.13 + Java 17
#  Base: eclipse-temurin:17-jdk-jammy (Yêu cầu JDK 17 cho Spark 4.x)
#  Vai trò: spark-master hoặc spark-worker (thiết lập qua biến SPARK_MODE)
# ============================================================

FROM eclipse-temurin:17-jdk-jammy

# --- Các biến môi trường ---
ENV SPARK_VERSION=4.1.1
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# --- Cài đặt các gói hệ thống cần thiết ---
# Cài đặt netcat-openbsd hỗ trợ lệnh kiểm tra cổng 'nc -z' trong entrypoint
RUN apt-get update && apt-get install -y \
    wget \
    python3 \
    python3-pip \
    procps \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# --- Cài đặt các thư viện Python ---
RUN pip3 install --no-cache-dir \
    pyspark==4.1.1 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    matplotlib==3.9.0 \
    seaborn==0.13.2 \
    python-dotenv==1.0.1

# --- Tải và cài đặt Spark 4.1.1 (Tương thích Hadoop 3 và Scala 2.13) ---
RUN wget --progress=bar:force:noscroll \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# --- Sao chép cấu hình và tập lệnh entrypoint ---
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# --- Thiết lập thư mục làm việc ---
RUN mkdir -p /opt/spark/work-dir
WORKDIR /opt/spark/work-dir

# --- Mở các cổng kết nối (Ports) ---
#  Master: 8080 (Giao diện Web UI), 7077 (RPC)
#  Worker: 8081 (Giao diện Web UI)
#  Ứng dụng: 4040 (Spark UI)
EXPOSE 8080 7077 8081 4040

ENTRYPOINT ["/entrypoint.sh"]