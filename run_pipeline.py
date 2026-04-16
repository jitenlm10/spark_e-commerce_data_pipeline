import subprocess

jobs = [
    "jobs/preprocess.py",
    "jobs/sessionization.py",
    "jobs/funnel_analysis.py",
    "jobs/attribution.py",
    "jobs/anomaly_detection.py",
    "jobs/export_frontend_data.py",
]

for job in jobs:
    print(f"\nRunning {job}...\n")
    result = subprocess.run(["python", job])

    if result.returncode != 0:
        print(f" Error in {job}. Stopping pipeline.")
        break

print("\nPipeline execution complete!")