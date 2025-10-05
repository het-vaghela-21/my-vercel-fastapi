# app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from pathlib import Path
import pandas as pd
import numpy as np

# --- Config ---
DATA_PATH = Path(__file__).parent / "data" / "telemetry.json"

# --- App & CORS (allow POST from any origin) ---
app = FastAPI(title="eShopCo Telemetry Metrics")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # allow any origin
    allow_methods=["POST"],        # only POST allowed
    allow_headers=["*"],
)

# --- Request model ---
class MetricsRequest(BaseModel):
    regions: List[str]
    threshold_ms: float

# --- Helper functions ---
def find_latency_column(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "latenc" in c.lower() or "ms" in c.lower():
            return c
    raise KeyError("No latency column found (expected name like 'latency' or 'latency_ms').")

def find_uptime_column(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "uptime" in c.lower() or c.lower() in ("up","is_up","available"):
            return c
    if "status" in df.columns:
        return "status"
    raise KeyError("No uptime column found (expected name like 'uptime' or 'status').")

def load_telemetry() -> pd.DataFrame:
    """Load telemetry data from JSON file."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Telemetry file not found at {DATA_PATH}")
    try:
        df = pd.read_json(DATA_PATH)
    except ValueError:
        # In case file is newline-delimited JSON
        df = pd.read_json(DATA_PATH, lines=True)
    return df

# --- Main endpoint ---
@app.post("/api/metrics")
def compute_metrics(req: MetricsRequest) -> Dict[str, Any]:
    df = load_telemetry()

    try:
        lat_col = find_latency_column(df)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        up_col = find_uptime_column(df)
    except KeyError:
        df["__uptime_dummy__"] = 1
        up_col = "__uptime_dummy__"

    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[up_col] = pd.to_numeric(df[up_col], errors="coerce")

    response = {}
    threshold = float(req.threshold_ms)

    # Determine region column name (some files use location/zone)
    region_col = None
    for c in df.columns:
        if c.lower() in ("region", "location", "zone"):
            region_col = c
            break
    if not region_col:
        raise HTTPException(status_code=400, detail="No region column found in telemetry data")

    for region in req.regions:
        sub = df[df[region_col].astype(str).str.lower() == region.lower()]
        if sub.empty:
            response[region] = {
                "avg_latency": 0.0,
                "p95_latency": 0.0,
                "avg_uptime": 0.0,
                "breaches": 0,
            }
            continue

        lat = sub[lat_col].dropna().astype(float)
        up = sub[up_col].dropna().astype(float)

        avg_latency = float(lat.mean())
        p95_latency = float(lat.quantile(0.95, interpolation="higher"))
        avg_uptime = float(up.mean()) if not up.empty else 0.0
        breaches = int((lat > threshold).sum())

        response[region] = {
            "avg_latency": round(avg_latency, 3),
            "p95_latency": round(p95_latency, 3),
            "avg_uptime": round(avg_uptime, 6),
            "breaches": breaches,
        }

    return response
