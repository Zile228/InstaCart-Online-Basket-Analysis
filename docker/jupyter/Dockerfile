# ============================================================
#  Jupyter Dockerfile
#  PATCH 5: python:3.11 base + pyspark==4.1.1 (not jupyter/pyspark-notebook)
#  Requires JDK 17 for PySpark 4.x
# ============================================================

FROM eclipse-temurin:17-jdk-jammy

# ── Environment ──────────────────────────────────────────────
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/opt/java/openjdk

# ── System packages ──────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Python packages ──────────────────────────────────────────
#  PATCH 1: pyspark==4.1.1
RUN pip3 install --no-cache-dir \
    pyspark==4.1.1 \
    jupyter \
    notebook \
    jupyterlab \
    pandas==2.2.2 \
    numpy==1.26.4 \
    matplotlib==3.9.0 \
    seaborn==0.13.2 \
    scikit-learn==1.5.0 \
    confluent-kafka==2.5.0 \
    python-dotenv==1.0.1 \
    pyarrow==16.1.0 \
    tqdm==4.66.4 \
    supabase==2.4.0

# ── Create non-root user ──────────────────────────────────────
RUN useradd -m -s /bin/bash -u 1000 nhom05
USER nhom05
WORKDIR /home/nhom05

# ── Create work directories ───────────────────────────────────
RUN mkdir -p /home/nhom05/work /home/nhom05/data

EXPOSE 8888

# ── Start Jupyter (no token for easy dev access) ─────────────
CMD ["python3", "-m", "jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--NotebookApp.token=''", \
     "--NotebookApp.password=''", \
     "--NotebookApp.notebook_dir=/home/nhom05/work"]
