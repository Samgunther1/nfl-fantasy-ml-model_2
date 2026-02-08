# src/get_nfl_data.py

import pandas as pd
import nflreadpy as nfl# example package
from pathlib import Path


# Choose season(s)
seasons = [2020,2021,2022,2023,2024,2025]

print("Downloading NFL data...")
df = nfl.load_player_stats(seasons)
df =df.to_pandas()

print("Data shape:", df.shape)

# Define output path relative to repo root
output_path = Path("data/processed/nfl_weekly_2020_2025.csv")

# Ensure directory exists
output_path.parent.mkdir(parents=True, exist_ok=True)

# Save file
df.to_csv(output_path, index=False)

print(f"Saved data to {output_path}")
