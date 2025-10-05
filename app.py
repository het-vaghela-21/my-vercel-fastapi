# app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from pathlib import Path
import pandas as pd
import numpy as np

# --- Config ---
DATA_PATH = Path(__file__).parent / "data" / "telemetry.csv"

# --- App & CORS (allow POST from any origin) ---
app = FastAPI(title="eShopCo Telemetry Metrics")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # allow any origin
    allow_methods=["POST"],        # only allow POST (per requirement)
    allow_headers=["*"],
)

# --- Request model ---
class MetricsRequest(BaseModel):
    regions: List[str]
    threshold_ms: float

# --- Helper: safe column discover ---
def find_latency_column(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "latenc" in c.lower() or "ms" in c.lower():
            return c
    raise KeyError("No latency column found (expected name like 'latency' or 'latency_ms').")

def find_uptime_column(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "uptime" in c.lower() or c.lower() in ("up","is_up","available"):
            return c
    # fallback to 'status' if 1/0 or 'ok' values exist
    if "status" in df.columns:
        return "status"
    raise KeyError("No uptime column found (expected name like 'uptime' or 'up').")

# --- Load telemetry (packaged with deployment) ---
def load_telemetry() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Telemetry file not found at {DATA_PATH}. Upload telemetry to that path.")
    df = pd.read_csv(DATA_PATH)
    return df

# --- Metrics endpoint ---
@app.post("/api/metrics")
def compute_metrics(req: MetricsRequest) -> Dict[str, Any]:
    df = load_telemetry()

    # find usable column names (makes the endpoint tolerant to slight CSV variations)
    try:
        lat_col = find_latency_column(df)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        up_col = find_uptime_column(df)
    except KeyError:
        # if no uptime is available, create a column of 1's (assume up)
        df["__uptime_dummy__"] = 1
        up_col = "__uptime_dummy__"

    # ensure numeric columns
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[up_col] = pd.to_numeric(df[up_col], errors="coerce")

    response = {}
    threshold = float(req.threshold_ms)

    for region in req.regions:
        sub = df[df.get("region", df.columns[0]).astype(str).str.lower() == region.lower()]
        # If dataset uses a different region column name, try common ones:
        if sub.empty and "region" not in df.columns:
            for rc in df.columns:
                if rc.lower() in ("region","loc","location","zone"):
                    sub = df[df[rc].astype(str).str.lower() == region.lower()]
                    break

        # compute metrics (handle empty)
        if sub.empty or sub[lat_col].dropna().empty:
            avg_latency = 0.0
            p95_latency = 0.0
            avg_uptime = 0.0
            breaches = 0
        else:
            lat = sub[lat_col].dropna().astype(float)
            avg_latency = float(lat.mean())
            # p95: pandas quantile is fine; convert to float
            p95_latency = float(lat.quantile(0.95, interpolation="higher"))
            up = sub[up_col].dropna().astype(float)
            if not up.empty:
                avg_uptime = float(up.mean())
            else:
                avg_uptime = 0.0
            breaches = int((lat > threshold).sum())

        response[region] = {
            "avg_latency": round(avg_latency, 3),
            "p95_latency": round(p95_latency, 3),
            "avg_uptime": round(avg_uptime, 6),  # keep precision for uptime fraction
            "breaches": breaches,
        }

    return response
