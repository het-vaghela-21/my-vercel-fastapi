# api/metrics.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from pathlib import Path
import pandas as pd
import numpy as np

app = FastAPI(title="eShopCo Telemetry Metrics")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST"],
    allow_headers=["*"],
)

class MetricsRequest(BaseModel):
    regions: List[str]
    threshold_ms: float

def load_telemetry() -> pd.DataFrame:
    path = Path(__file__).parent.parent / "data" / "telemetry.json"
    try:
        df = pd.read_json(path)
    except ValueError:
        df = pd.read_json(path, lines=True)
    return df

@app.post("/")
def compute_metrics(req: MetricsRequest) -> Dict[str, Any]:
    df = load_telemetry()
    for c in df.columns:
        if "region" in c.lower():
            region_col = c
            break
    else:
        raise HTTPException(status_code=400, detail="No region column found")

    for c in df.columns:
        if "latenc" in c.lower():
            lat_col = c
            break
    else:
        raise HTTPException(status_code=400, detail="No latency column found")

    up_col = next((c for c in df.columns if "uptime" in c.lower()), None)
    if up_col is None:
        df["uptime"] = 1.0
        up_col = "uptime"

    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[up_col] = pd.to_numeric(df[up_col], errors="coerce")

    results = {}
    for region in req.regions:
        sub = df[df[region_col].astype(str).str.lower() == region.lower()]
        if sub.empty:
            results[region] = {"avg_latency": 0, "p95_latency": 0, "avg_uptime": 0, "breaches": 0}
            continue
        lat = sub[lat_col].dropna()
        avg_latency = lat.mean()
        p95_latency = lat.quantile(0.95, interpolation="higher")
        avg_uptime = sub[up_col].mean()
        breaches = int((lat > req.threshold_ms).sum())
        results[region] = {
            "avg_latency": round(avg_latency, 2),
            "p95_latency": round(p95_latency, 2),
            "avg_uptime": round(avg_uptime, 4),
            "breaches": breaches,
        }
    return results
