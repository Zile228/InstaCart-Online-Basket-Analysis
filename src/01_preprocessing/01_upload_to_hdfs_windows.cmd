@echo off
:: ============================================================
::  01_upload_to_hdfs_windows.cmd
::  Upload Instacart CSV lên HDFS — chạy từ Windows host
::  Prerequisites: Docker đang chạy, namenode container UP
:: ============================================================

setlocal

SET DATA_DIR=..\data
SET NAMENODE_CONTAINER=namenode

echo ======================================================
echo   Instacart — Copy CSV files into NameNode container
echo ======================================================
echo.

:: ── STEP 1: Copy files into namenode /tmp/ ────────────────
echo [1/3] Copying CSV files into container %NAMENODE_CONTAINER%:/tmp/ ...
echo.

for %%F in (orders.csv order_products__prior.csv order_products__train.csv products.csv aisles.csv departments.csv) do (
    if exist "%DATA_DIR%\%%F" (
        echo   Copying %%F ...
        docker cp "%DATA_DIR%\%%F" %NAMENODE_CONTAINER%:/tmp/%%F
        echo     OK
    ) else (
        echo   WARNING: %DATA_DIR%\%%F not found! Skipping.
    )
)

echo.
echo [2/3] Running HDFS upload script inside container ...
echo.

:: ── STEP 2: Run upload script inside container ─────────────
docker exec %NAMENODE_CONTAINER% bash -c ^
  "DATA_DIR=/tmp hdfs dfs -mkdir -p /instacart/raw /instacart/features /instacart/models /instacart/streaming /spark-logs && ^
   hdfs dfs -put -f /tmp/orders.csv /instacart/raw/ && ^
   hdfs dfs -put -f /tmp/order_products__prior.csv /instacart/raw/ && ^
   hdfs dfs -put -f /tmp/order_products__train.csv /instacart/raw/ && ^
   hdfs dfs -put -f /tmp/products.csv /instacart/raw/ && ^
   hdfs dfs -put -f /tmp/aisles.csv /instacart/raw/ && ^
   hdfs dfs -put -f /tmp/departments.csv /instacart/raw/ && ^
   echo === Upload Done === && ^
   hdfs dfs -ls /instacart/raw/ && ^
   hdfs dfs -du -h /instacart/"

:: ── STEP 3: Verify ─────────────────────────────────────────
echo.
echo [3/3] Verifying upload ...
docker exec %NAMENODE_CONTAINER% hdfs dfsadmin -report

echo.
echo ======================================================
echo   Done! Open http://localhost:9870 to verify in UI.
echo ======================================================
endlocal
pause
