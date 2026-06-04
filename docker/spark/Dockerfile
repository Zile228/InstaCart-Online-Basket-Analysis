# ============================================================
#  Spark 4.1.1 — Custom Image
#  PATCH 1 & 6: Spark 4.1.1 + Scala 2.13 + Java 17
#  Base: eclipse-temurin:17-jdk-jammy (JDK 17 required for Spark 4.x)
#  Roles: spark-master OR spark-worker (via SPARK_MODE env var)
# ============================================================

FROM eclipse-temurin:17-jdk-jammy

# ── Environment ──────────────────────────────────────────────
ENV SPARK_VERSION=4.1.1
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# ── System packages ──────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    wget \
    python3 \
    python3-pip \
    procps \
    && rm -rf /var/lib/apt/lists/*

# ── Python packages ──────────────────────────────────────────
#  PATCH 1: pyspark==4.1.1 (not 3.5.1)
RUN pip3 install --no-cache-dir \
    pyspark==4.1.1 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    matplotlib==3.9.0 \
    seaborn==0.13.2 \
    confluent-kafka==2.5.0 \
    python-dotenv==1.0.1

# ── Download and install Spark 4.1.1 ─────────────────────────
#  Built for Hadoop 3, Scala 2.13 (PATCH 1)
RUN wget -q \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# ── Spark config and entrypoint ───────────────────────────────
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ── Work directory ─────────────────────────────────────────
RUN mkdir -p /opt/spark/work-dir
WORKDIR /opt/spark/work-dir

# ── Exposed ports ─────────────────────────────────────────────
#  Master: 8080 (Web UI), 7077 (RPC)
#  Worker: 8081 (Web UI)
#  Application: 4040 (Spark UI)
EXPOSE 8080 7077 8081 4040

ENTRYPOINT ["/entrypoint.sh"]
