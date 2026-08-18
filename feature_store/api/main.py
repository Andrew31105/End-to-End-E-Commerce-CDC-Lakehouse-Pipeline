"""
============================================================================
 feature_store/api/main.py
 Feature Store API – FastAPI service phục vụ features cho ML/BI
============================================================================
 Exposes:
   - GET  /features/{feature_group}   – Lấy feature values theo key
   - POST /features/ingest             – (optional) ingest external features
   - GET  /health                      – Health check
============================================================================
 (PLACEHOLDER – Code triển khai sau)
"""
from fastapi import FastAPI

app = FastAPI(
    title="Feature Store API",
    description="Serving layer for Feature Store / Lakehouse",
    version="0.1.0",
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "feature-store-api"}


# TODO: GET /features/{feature_group}?entity_id=...
# TODO: POST /features/ingest
# TODO: Kết nối ClickHouse & Trino để query features real-time & batch
