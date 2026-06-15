#!/bin/bash
# ============================================================
#  01_upload_to_hdfs.sh
#  Upload Instacart CSV files vào HDFS
#
#  Biến môi trường:
#    DATA_DIR  - thư mục chứa CSV (mặc định: /home/nhom05/data)
#    HDFS_DIR  - đích trên HDFS  (mặc định: /instacart/raw)
# ============================================================

set -e

DATA_DIR="${DATA_DIR:-/home/nhom05/data}"
HDFS_DIR="${HDFS_DIR:-/instacart/raw}"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}======================================================"
echo -e "  Instacart Data Upload to HDFS"
echo -e "  Data source : ${DATA_DIR}"
echo -e "  HDFS target : ${HDFS_DIR}"
echo -e "======================================================${NC}"
echo ""

# STEP 1: Create HDFS directories
echo -e "${YELLOW}[STEP 1/4] Creating HDFS directory structure...${NC}"
hdfs dfs -mkdir -p /instacart/raw
hdfs dfs -mkdir -p /instacart/features
hdfs dfs -mkdir -p /instacart/models
hdfs dfs -mkdir -p /instacart/streaming
hdfs dfs -mkdir -p /spark-logs
echo -e "${GREEN}   HDFS directories created${NC}"
echo ""

# STEP 2: Upload CSV files
echo -e "${YELLOW}[STEP 2/4] Uploading CSV files to HDFS...${NC}"
echo ""

UPLOAD_SUCCESS=0
UPLOAD_FAIL=0

FILES=(
    "orders.csv"
    "order_products__prior.csv"
    "order_products__train.csv"
    "products.csv"
    "aisles.csv"
    "departments.csv"
)

for FILE in "${FILES[@]}"; do
    LOCAL_PATH="${DATA_DIR}/${FILE}"
    HDFS_PATH="${HDFS_DIR}/${FILE}"

    if [ -f "${LOCAL_PATH}" ]; then
        echo -n "  Uploading ${FILE} ... "
        # -f flag: overwrite if exists
        hdfs dfs -put -f "${LOCAL_PATH}" "${HDFS_PATH}"
        SIZE=$(du -h "${LOCAL_PATH}" | cut -f1)
        LINES=$(wc -l < "${LOCAL_PATH}")
        echo -e "${GREEN}   done (local size: ${SIZE}, lines: ${LINES})${NC}"
        UPLOAD_SUCCESS=$((UPLOAD_SUCCESS + 1))
    else
        echo -e "${RED}   NOT FOUND: ${LOCAL_PATH}${NC}"
        UPLOAD_FAIL=$((UPLOAD_FAIL + 1))
    fi
done

echo ""
echo -e "  Uploaded: ${GREEN}${UPLOAD_SUCCESS}${NC} files, Failed: ${RED}${UPLOAD_FAIL}${NC} files"
echo ""

# Exit early if uploads failed
if [ "${UPLOAD_FAIL}" -gt 0 ]; then
    echo -e "${RED}WARNING: Some files failed to upload!"
    echo "  Kiểm tra lại folder data/ trên host đã có đủ 6 file CSV chưa."
    echo "  Volume mount: ../data → /home/nhom05/data (trong namenode)"
    echo -e "${NC}"
fi

# STEP 3: Verify upload 
echo -e "${YELLOW}[STEP 3/4] Verifying files on HDFS...${NC}"
echo ""
echo "=== Files in ${HDFS_DIR}/ ==="
hdfs dfs -ls "${HDFS_DIR}/"
echo ""

echo "=== File sizes (human readable) ==="
hdfs dfs -du -h "${HDFS_DIR}/"
echo ""

echo "=== Total instacart directory ==="
hdfs dfs -du -h -s /instacart/
echo ""

# STEP 4: DataNode health check  
echo -e "${YELLOW}[STEP 4/4] Checking HDFS cluster health...${NC}"
hdfs dfsadmin -report | head -30
echo ""

echo -e "${BLUE}======================================================"
echo -e "  Upload Complete!"
if [ "${UPLOAD_FAIL}" -eq 0 ]; then
    echo -e "  ${GREEN}All 6 CSV files are on HDFS   ${NC}"
else
    echo -e "  ${RED}WARNING: ${UPLOAD_FAIL} file(s) missing${NC}"
fi
echo -e "======================================================${NC}"