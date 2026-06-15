# ============================================================
#  Jupyter Dockerfile
#  Base: eclipse-temurin:17 + pyspark==4.1.1
#  Lưu ý:
#    - Chạy bằng quyền root để tránh lỗi phân quyền khi ghi file vào volume mount
#    - Sử dụng tham số cấu hình ServerApp phù hợp cho Jupyter 7.x
# ============================================================

FROM eclipse-temurin:17-jdk-jammy

# --- Các biến môi trường ---
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/opt/java/openjdk

# --- Cài đặt các gói hệ thống ---
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# --- Cài đặt các thư viện Python ---
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

# --- Thiết lập thư mục làm việc ---
RUN mkdir -p /home/nhom05/work /home/nhom05/data

WORKDIR /home/nhom05

EXPOSE 8888

# --- Khởi chạy Jupyter Notebook 7.x ---
# Sử dụng tham số --ServerApp thay thế cho --NotebookApp từ phiên bản 7.x
CMD ["python3", "-m", "jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--ServerApp.token=''", \
     "--ServerApp.password=''", \
     "--ServerApp.root_dir=/home/nhom05/work"]