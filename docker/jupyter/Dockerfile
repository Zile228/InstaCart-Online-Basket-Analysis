# ============================================================
#  Jupyter Dockerfile
#  PATCH 5: eclipse-temurin:17 base + pyspark==4.1.1
#
#  BUG FIX (v2):
#    1. Chạy bằng root (xóa USER jovyan) → tránh permission
#       denied khi Jupyter ghi file vào volume mount từ host
#    2. Đổi --NotebookApp.* → --ServerApp.* (Jupyter 7.x đổi
#       tên config, NotebookApp.notebook_dir bị bỏ qua)
#    3. Pin notebook==7.2.2 để tránh breaking changes tương lai
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
    python-dotenv==1.0.1 \
    pyarrow==17.0.0 \
    tqdm==4.66.4 \
    supabase==2.4.0

# ── Work directories (chạy root, không cần chown) ────────────
# BUG FIX: Bỏ useradd + USER jovyan. Chạy root trong container
# là acceptable cho môi trường dev/demo. Nếu cần bảo mật hơn,
# dùng: docker-compose.yml → user: "${UID}:${GID}"
# NOTE: Thư mục đổi từ /home/jovyan → /home/nhom05
RUN mkdir -p /home/nhom05/work /home/nhom05/data

WORKDIR /home/nhom05

EXPOSE 8888

# ── Start Jupyter Notebook 7.x ────────────────────────────────
# BUG FIX: Dùng --ServerApp.* thay --NotebookApp.* (Jupyter 7.x)
# notebook_dir → root_dir, token/password giữ nguyên tên nhưng
# nay nằm dưới ServerApp (hoặc IdentityProvider)
CMD ["python3", "-m", "jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--ServerApp.token=''", \
     "--ServerApp.password=''", \
     "--ServerApp.root_dir=/home/nhom05/work"]
