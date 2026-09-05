"""
============================================================================
 ml/predict_next.py
 Online inference demo – dự đoán quy mô đơn/doanh thu 1 phút tới cho 1 sản phẩm
============================================================================
 Dòng chảy (minh hoạ Serving Layer cho ML):
   1. Gọi Feature Store API  GET /features/order_features?entity_id=<pid>
      -> lấy feature window mới nhất (current minute).
   2. Lấy thêm lag trước đó từ ClickHouse (cần lag1 & lag2 cho model).
   3. Dựng vector feature (hour/minute của phút tới + các lag) khớp với
      feature list đã lưu khi train.
   4. Load model RandomForest đã train -> predict total_orders_1min & revenue.

 Chạy:
   conda run -n crypto python ml/predict_next.py --product-id 3
============================================================================
"""
import argparse
import datetime as dt
import os
from pathlib import Path

import joblib
import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv

load_dotenv()

MODEL_DIR = Path(__file__).resolve().parent / "models"
FEATURE_API = os.getenv("FEATURE_API_URL", "http://127.0.0.1:8000")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9002"))
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "ecommerce")


def fetch_lags(product_id: int) -> dict:
    """2 dòng window gần nhất của sản phẩm -> các cột lag1/lag2."""
    client = Client(host=CH_HOST, port=CH_PORT, user=CH_USER,
                    password=CH_PASSWORD, database=CH_DB)
    try:
        rows, cols = client.execute(
            """
            SELECT window_start, total_orders_1min, total_revenue_1min,
                   status_changed_count
            FROM product_metrics_1min
            WHERE product_id = %(pid)s
            ORDER BY window_start DESC
            LIMIT 2
            """,
            {"pid": int(product_id)},
            with_column_types=True,
        )
    finally:
        client.disconnect()
    if not rows:
        return {}
    cols = [c[0] for c in cols]
    latest, prev = rows[0], (rows[1] if len(rows) > 1 else None)
    lag1 = dict(zip(cols, latest))
    lag2 = dict(zip(cols, prev)) if prev else {c: 0 for c in cols}
    return {"lag1": lag1, "lag2": lag2}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--product-id", type=int, default=3)
    args = ap.parse_args()
    pid = args.product_id

    # 1) Feature Store API (minh hoạ dùng serving layer)
    import json
    import urllib.request
    url = f"{FEATURE_API}/features/order_features?entity_id={pid}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            api_feat = json.loads(r.read()).get("features", {})
        print("Feature API order_features:", api_feat)
    except Exception as exc:  # noqa: BLE001
        print("(Feature API không truy cập được, vẫn dùng ClickHouse trực tiếp)",
              exc)

    # 2) lags từ ClickHouse
    l = fetch_lags(pid)
    if not l:
        print(f"Không có dữ liệu window cho product_id={pid}"); return
    lag1, lag2 = l["lag1"], l["lag2"]

    # 3) feature vector cho "phút tới"
    nxt = dt.datetime.now() + dt.timedelta(minutes=1)
    feat = {
        "hour": float(nxt.hour), "minute": float(nxt.minute),
        "total_orders_1min_lag1": float(lag1["total_orders_1min"]),
        "total_revenue_1min_lag1": float(lag1["total_revenue_1min"]),
        "status_changed_count_lag1": float(lag1["status_changed_count"]),
        "total_orders_1min_lag2": float(lag2["total_orders_1min"]),
        "total_revenue_1min_lag2": float(lag2["total_revenue_1min"]),
        "status_changed_count_lag2": float(lag2["status_changed_count"]),
    }

    for name in ("orders_1min", "revenue_1min"):
        p = MODEL_DIR / f"product_{name}_model.joblib"
        if not p.exists():
            print(f"Thiếu model {p} (chạy ml/train_realtime_model.py trước)"); continue
        obj = joblib.load(p)
        X = pd.DataFrame([[feat[f] for f in obj["features"]]],
                         columns=obj["features"])
        pred = float(obj["model"].predict(X)[0])
        unit = "đơn" if name == "orders_1min" else "VNĐ"
        print(f"Dự đoán {name} phút tới (product {pid}): "
              f"{max(0, round(pred)) if name=='orders_1min' else round(pred):,} {unit}")


if __name__ == "__main__":
    main()
