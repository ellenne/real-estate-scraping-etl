import os
import subprocess
from pathlib import Path

# File paths
db_file = Path("pricehubble.db")
csv_file = Path("property_offers.csv")

# Step 1: Remove old output files
def clean_outputs():
    if db_file.exists():
        db_file.unlink()
        print("Removed existing pricehubble.db")
    else:
        print("No existing DB file to remove.")

    if csv_file.exists():
        csv_file.unlink()
        print("Removed existing property_offers.csv")
    else:
        print("No existing CSV file to remove.")

# Step 2: Run the ETL pipeline
def run_pipeline():
    print("Running ETL pipeline (pipeline.py)...")
    result = subprocess.run(["python", "pipeline.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print("Error running pipeline.py:")
        print(result.stderr)
        exit(1)

# Step 3: Run the export script
def export_csv():
    print("Running explore.py to export CSV...")
    result = subprocess.run(["python", "explore.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print("Error running explore.py:")
        print(result.stderr)
        exit(1)

# Step 4: Confirm results
def confirm_output():
    if csv_file.exists():
        size = os.path.getsize(csv_file)
        print(f"Export complete: {csv_file.name} ({size:,} bytes)")
    else:
        print("CSV file was not created.")

if __name__ == "__main__":
    print("Starting full reset and ETL run...\n")
    clean_outputs()
    run_pipeline()
    export_csv()
    confirm_output()
    print("\n Done.")
