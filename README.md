
***

```markdown
# Distributed E-Commerce Data Analytics Pipeline

## Project Overview
---

## Initial Setup (Required)
**You must download the dataset before running the pipeline.**

1. **Download the Dataset:** Download the raw CSV files from Kaggle here: [Marketing and E-Commerce Analytics Dataset](https://www.kaggle.com/datasets/geethasagarbonthu/marketing-and-e-commerce-analytics-dataset?select=events.csv).
2. **Create the Data Folder:** Create a new folder named `data` in the root directory of this project.
3. **Insert the Files:** Extract the downloaded files and place `events.csv`, `transactions.csv`, and `products.csv` directly into the `data` folder.

Your repository structure should look like this before pipeline execution:

```text
📦 project_root
 ┣ 📂 data                     # YOU MUST CREATE THIS: Place Kaggle CSVs here
 ┃ ┣ 📜 events.csv             
 ┃ ┣ 📜 transactions.csv       
 ┃ ┗ 📜 products.csv           
 ┣ 📂 jobs                     # PySpark executable scripts
 ┃ ┣ 📜 inflate_data.py        # Distributed data scaling utility
 ┃ ┣ 📜 preprocess.py          # Stage 1
 ┃ ┣ 📜 sessionization.py      # Stage 2
 ┃ ┣ 📜 funnel_analysis.py     # Stage 3
 ┃ ┣ 📜 attribution.py         # Stage 4
 ┃ ┗ 📜 anomaly_detection.py   # Stage 5
 ┣ 📜 docker-compose.yml       # Cluster infrastructure definition
 ┣ 📜 run_pipeline.ps1         # Pipeline orchestrator
 ┗ 📜 .gitignore               # Excludes local dependencies
```
*(Note: The `/cleaned` and `/output` folders will be generated automatically by the pipeline during execution).*

---

## System Prerequisites
This project relies entirely on containerization to ensure strict reproducibility. **No local dependencies, Python environments, or Spark installations are required.** You only need:
1. **Docker Desktop** (Running in the background)
2. **PowerShell** or any standard terminal.

---

## Step-by-Step Execution Guide

### Phase 1: Bootstrapping the Cluster
Open your terminal in the root directory of this project and execute the following command to provision the Spark Master and Worker nodes, alongside the Jupyter environment:

```bash
docker-compose up -d
```
*Note: You can verify the cluster is active by running `docker ps` to ensure `spark-master`, `spark-worker-1`, `spark-worker-2`, and `jupyter` are running.*

### Phase 2: Massive Data Generation (One-Time Setup)
To evaluate the pipeline at a distributed scale, you must first generate the 40-million-row (4 GB) dataset. **This command only needs to be run once.** It uses distributed array explosion to scale the baseline CSVs into massive datasets optimized for stress-testing the network.

```bash
docker exec spark-master spark-submit /opt/bitnami/spark/project/jobs/inflate_data.py
```
*Wait for this job to complete. It will save the massively scaled datasets into your `/data` folder.*

### Phase 3: Pipeline Orchestration
With the data prepped and the cluster running, you can now execute the end-to-end analytical pipeline. Run the orchestration script:

```powershell
.\run_pipeline.ps1
```
*(If you encounter an execution policy error in Windows, run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` first).*

This script will sequentially submit the 5 core PySpark jobs to the Master node. Each stage writes its intermediate state to disk as partitioned Parquet files before the next stage begins.

---

## Monitoring Distributed Execution (Spark UI)
While `run_pipeline.ps1` is executing, you can monitor the distributed cluster's real-time performance:

1. Open your web browser and navigate to the Spark Master UI: **`http://localhost:8080`**
2. **What to look for:**
   * Under **Workers**, you will see both nodes actively sharing the computational load.
   * Under **Completed Applications**, you can view the exact execution duration for each pipeline stage, demonstrating the performance variations between stateful and stateless distributed operations.

---

## Business Analytics & Results (Jupyter Localhost)
Once the pipeline finishes executing, the fully aggregated and compressed Parquet files will be dynamically generated inside an `/output` folder. 

To view the final business metrics (Conversion Funnels, Revenue Attribution, and Anomaly Detection), we will use the dedicated Jupyter container built into the cluster:

1. Check the terminal where you initially ran the `docker-compose` command. Look for a log line that says `Jupyter Server is running at:`.
2. Copy the URL provided in the logs that looks similar to this: **`http://127.0.0.1:8888/lab?token=<your_unique_token>`** and paste it into your web browser. *(If you closed the terminal window, you can retrieve the link at any time by running `docker logs jupyter`)*.
3. In the Jupyter web interface, locate and open **`output_visualization.ipynb`**.
4. In the top menu bar, click **Run > Run All Cells** (or the double-play `Restart & Run All` button).
5. Pandas and Matplotlib will ingest the finalized PySpark outputs to instantly generate the analytical charts and the distributed scaling performance comparisons.

---

```
```
