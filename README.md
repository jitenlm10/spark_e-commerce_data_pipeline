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
3. In the Jupyter web interface, locate notebook folder and open **`output_visualization.ipynb`**.
4. In the top menu bar, click **Run > Run All Cells** (or the double-play `Restart & Run All` button).
5. Pandas and Matplotlib will ingest the finalized PySpark outputs to instantly generate the analytical charts and the distributed scaling performance comparisons.

---

```
```

# Frontend

# Spark E-Commerce Data Pipeline

A scalable e-commerce analytics pipeline built using Apache Spark, FastAPI, and React.


## Overview

This project processes e-commerce event data (views, carts, purchases) and generates analytics such as:
- Funnel conversion rates
- Attribution metrics
- Anomaly detection

The system consists of:
1. Spark Data Pipeline
2. FastAPI Backend
3. React Frontend

---

## Project Structure
backend/ → FastAPI backend
frontend/ → React frontend
jobs/ → Spark processing scripts
output/ → Generated JSON outputs
run_pipeline.py → Pipeline orchestrator

## How the System Works

1. Raw CSV data is processed using Spark jobs
2. Pipeline generates JSON outputs in `output/`
3. FastAPI backend reads these files and exposes APIs
4. React frontend fetches APIs and displays dashboard

---

## Frontend (How It Was Built)

The frontend is built using **React + Vite**.

### Key files:
- `frontend/src/App.jsx` → main app
- `frontend/src/Dashboard.jsx` → charts + UI
- `frontend/src/HeroGuide.jsx` → interactive guide
- `frontend/src/api.js` → API calls to backend

### How it works:
- `api.js` calls backend endpoints:
  - `/api/summary`
  - `/api/funnel`
  - `/api/anomalies`
- Data is fetched using `fetch()` or axios
- Dashboard components render charts and analytics
- UI is built using React components and state

---

## Setup Instructions

### 1. Clone repo

```bash
git clone https://github.com/jitenlm10/spark_e-commerce_data_pipeline.git
cd spark_e-commerce_data_pipeline

## Frontend Implementation

The frontend was developed using **React** and **JavaScript** to provide an interactive dashboard for visualizing analytics generated by the Spark pipeline.

### Technologies Used
- **React** for component-based UI development
- **JavaScript** for frontend logic and API integration
- **Vite** for frontend build and development server
- **Ollama** for local LLM-based chat and explanation support
- **Blender** for preparing and exporting the 3D guide character model

### How the Frontend Was Built

The frontend was designed as a React application with modular components for dashboard rendering and user interaction.

Main frontend files include:
- `App.jsx` for the main application structure
- `Dashboard.jsx` for charts and analytics display
- `HeroGuide.jsx` for the interactive guide character
- `api.js` for communication with backend APIs

### Ollama Integration

Ollama was used to support AI-driven responses in the frontend. The frontend sends user questions to the backend, and the backend forwards the request to Ollama for response generation. This allows users to ask questions about the dashboard and analytics results in natural language.

### Blender Character Integration

A 3D character was prepared using **Blender** and integrated into the frontend as a guide/avatar component. The model was used to improve user interaction and presentation quality. The React frontend renders this character as part of the user interface, making the dashboard more engaging and visually interactive.

### Frontend Workflow

1. The Spark pipeline generates analytics results.
2. The backend exposes the results through API endpoints.
3. The React frontend fetches this data using JavaScript.
4. Dashboard components display the analytics visually.
5. User questions can be passed through the backend to Ollama.
6. The Blender-based guide character is shown as part of the frontend interface.
