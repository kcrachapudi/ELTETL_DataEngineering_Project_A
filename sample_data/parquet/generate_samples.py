"""
Run this once on your machine to generate the Parquet sample files.

    pip install pyarrow
    python sample_data/parquet/generate_samples.py
"""

import random
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

out = Path(__file__).parent
out.mkdir(exist_ok=True)

random.seed(42)

# ── Weather observations ───────────────────────────────────────────────────
rows = []
base     = datetime(2024, 1, 1)
stations = ["WS-001", "WS-002", "WS-003", "WS-004", "WS-005"]

for day in range(7):
    for hour in range(24):
        ts = base + timedelta(days=day, hours=hour)
        for station in stations:
            rows.append({
                "observation_ts":   ts,
                "station_id":       station,
                "temperature_f":    round(35 + random.uniform(-5, 30), 1),
                "humidity_pct":     round(random.uniform(40, 85), 1),
                "precipitation_in": round(random.uniform(0, 0.1), 3),
                "windspeed_mph":    round(random.uniform(3, 20), 1),
                "weathercode":      random.choice([0, 0, 0, 1, 1, 2, 3]),
                "data_source":      "open_meteo",
                "load_ts":          datetime(2024, 1, 8, 6, 0, 0),
            })

weather_df = pd.DataFrame(rows)
weather_df["observation_ts"] = pd.to_datetime(weather_df["observation_ts"])
weather_df["load_ts"]        = pd.to_datetime(weather_df["load_ts"])
weather_df.to_parquet(out / "weather_observations.parquet", index=False)
print(f"Created weather_observations.parquet — {len(weather_df)} rows")

# ── Claims export ──────────────────────────────────────────────────────────
claims_df = pd.DataFrame([
    {"claim_id": "CLM-2024-001", "patient_id": "MBR-10003",
     "provider_npi": "1234567890", "service_date": "2024-01-10",
     "procedure_code": "99213", "charge_amount": 150.00,
     "allowed_amount": 121.25, "paid_amount": 121.25,
     "patient_responsibility": 0.00, "claim_status": "processed_primary",
     "payer_id": "BCBS001", "diagnosis_1": "J069", "diagnosis_2": "Z2389"},
    {"claim_id": "CLM-2024-001", "patient_id": "MBR-10003",
     "provider_npi": "1234567890", "service_date": "2024-01-10",
     "procedure_code": "87804", "charge_amount": 75.00,
     "allowed_amount": 63.50, "paid_amount": 63.50,
     "patient_responsibility": 0.00, "claim_status": "processed_primary",
     "payer_id": "BCBS001", "diagnosis_1": "J069", "diagnosis_2": "Z2389"},
    {"claim_id": "CLM-2024-002", "patient_id": "MBR-20001",
     "provider_npi": "9876543210", "service_date": "2024-01-12",
     "procedure_code": "99214", "charge_amount": 200.00,
     "allowed_amount": 175.00, "paid_amount": 125.00,
     "patient_responsibility": 50.00, "claim_status": "processed_primary",
     "payer_id": "BCBS001", "diagnosis_1": "N18.9", "diagnosis_2": "I10"},
    {"claim_id": "CLM-2024-002", "patient_id": "MBR-20001",
     "provider_npi": "9876543210", "service_date": "2024-01-12",
     "procedure_code": "36415", "charge_amount": 25.00,
     "allowed_amount": 25.00, "paid_amount": 25.00,
     "patient_responsibility": 0.00, "claim_status": "processed_primary",
     "payer_id": "BCBS001", "diagnosis_1": "N18.9", "diagnosis_2": "I10"},
])
claims_df.to_parquet(out / "claims_export.parquet", index=False)
print(f"Created claims_export.parquet — {len(claims_df)} rows")

print("\nDone. Both .parquet files are in sample_data/parquet/")
