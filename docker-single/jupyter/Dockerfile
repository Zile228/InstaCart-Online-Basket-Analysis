# Dockerfile cau hinh Jupyter Notebook ho tro PySpark
# Su dung base image eclipse-temurin:17 va cai san pyspark 4.1.1

FROM eclipse-temurin:17-jdk-jammy

# Thiet lap cac bien moi truong cho Java va Python
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/opt/java/openjdk

# Cai dat cac thu vien he thong co ban
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Cai dat cac package Python phuc vu cho phan tich du lieu va lam viec voi Spark
RUN pip3 install --no-cache-dir \
    pyspark==4.1.1 \
    "jupyter_server>=2.0" \
    "notebook==7.2.2" \
    jupyterlab \
    pandas==2.2.2 \
    numpy >=2.0.0 \
    matplotlib==3.9.0 \
    seaborn==0.13.2 \
    scikit-learn==1.5.0 \
    confluent-kafka==2.5.0 \
    python-dotenv==1.0.1 \
    pyarrow==17.0.0 \
    tqdm==4.66.4 \
    supabase==2.4.0

# Tao va thiet lap thu muc lam viec trong container
RUN mkdir -p /home/nhom05/work /home/nhom05/data

WORKDIR /home/nhom05

EXPOSE 8888

# Khoi dong Jupyter Notebook tren port 8888 va mo thu muc lam viec mac dinh
CMD ["python3", "-m", "jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--ServerApp.token=''", \
     "--ServerApp.password=''", \
     "--ServerApp.root_dir=/home/nhom05/work"]