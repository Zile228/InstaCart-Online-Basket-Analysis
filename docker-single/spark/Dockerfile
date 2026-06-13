# Spark 4.1.1 - Image dung chung cho ca Master va Worker
# Base image: eclipse-temurin:17-jdk-jammy (Spark 4.x bat buoc phai dung JDK 17)
# Vai tro cua tung container duoc quyet dinh qua bien moi truong SPARK_MODE (spark-master hoac spark-worker)

FROM eclipse-temurin:17-jdk-jammy

# Thiet lap cac bien moi truong can thiet cho Java, Python va Spark
ENV SPARK_VERSION=4.1.1
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Cai dat cac package he thong va cong cu dong lenh co ban
RUN apt-get update && apt-get install -y \
    wget \
    python3 \
    python3-pip \
    procps \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Cai dat PySpark va cac thu vien Python phuc vu phan tich du lieu
RUN pip3 install --no-cache-dir \
    pyspark==4.1.1 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    matplotlib==3.9.0 \
    seaborn==0.13.2 \
    confluent-kafka==2.5.0 \
    python-dotenv==1.0.1

# Tai va giai nen phien ban Spark 4.1.1 (phien ban build cho Hadoop 3 va Scala 2.13)
RUN wget --progress=bar:force:noscroll \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Copy file cau hinh mac dinh va set quyen cho file entrypoint khoi dong
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Thiet lap thu muc lam viec mac dinh
RUN mkdir -p /opt/spark/work-dir
WORKDIR /opt/spark/work-dir

# Mo cac port ket noi va quan ly:
# - Master: 8080 (Web UI), 7077 (RPC)
# - Worker: 8081 (Web UI)
# - Application: 4040 (Spark UI)
EXPOSE 8080 7077 8081 4040

ENTRYPOINT ["/entrypoint.sh"]