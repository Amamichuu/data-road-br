import os
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up the data directory relative to the project
data_dir = Path("../data").resolve()
data_dir.mkdir(parents=True, exist_ok=True)

# Kaggle dataset reference
dataset = "codebynadiia/gdp-1975-2025"

# Get Kaggle credentials from environment
kaggle_username = "kelasi"#os.getenv("KAGGLE_USERNAME")
kaggle_key = "902b8749c2bf482d18da5520c9e7908e"#os.getenv("KAGGLE_KEY")

if not kaggle_username or not kaggle_key:
    raise ValueError("KAGGLE_USERNAME and KAGGLE_KEY not found in environment variables or .env file")

os.environ['KAGGLE_USERNAME'] = kaggle_username
os.environ['KAGGLE_KEY'] = kaggle_key

# Check if the CSV already exists
csv_file = data_dir / "gdp_by_country_1975_2025.csv"
if csv_file.exists():
    print(f"✅ Dataset already exists at {csv_file}, skipping download.")
else:
    # Download and unzip the dataset to the data directory
    subprocess.run([
        "kaggle", "datasets", "download",
        "-d", dataset,
        "--unzip",
        "-p", str(data_dir)
    ], check=True)
    print(f"✅ Dataset downloaded and extracted to {data_dir}!")