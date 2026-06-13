# Notebooks

## `05_colab_full_mllib_run.ipynb`

Google Colab notebook for running the MLlib pipeline before the team master
cluster is available.

Purpose:

- Clone this repo in Colab.
- Upload/download the Instacart CSV files.
- Run `src/03_ml/local_train_mllib.py` with a fixed seed.
- Export reports, features and models as a zip.

Use `SAMPLE_FRACTION = 0.02` for a smoke test and `SAMPLE_FRACTION = 1.0`
for the full dataset.

The Colab output is intended for early report writing and sanity checking. The
official team run should still be produced by the HDFS/Spark scripts under
`src/01_preprocessing` and `src/03_ml` once the master cluster is available.
