"""
============================================================================
 ml/train_realtime_model.py
 ML demo – dự đoán quy mô đơn/doanh thu của sản phẩm trong 1 phút tới
============================================================================
 Nguồn feature: ClickHouse `ecommerce.product_metrics_1min` (Flink real-time)
   - mỗi dòng = (product_id, window_start, total_orders_1min, total_revenue_1min,
     avg_order_value_1min, status_changed_count)

 Cách làm (demo pipeline):
   1. Đọc data ClickHouse qua clickhouse-driver (native, host port 9002).
   2. Feature engineering:
        - lag 1,2 phút của total_orders_1min / total_revenue_1min / status_changed
        - thời gian: hour, minute
      (không dùng product_id -> model học theo quy luật chung, phòng khi sản phẩm mới)
   3. Chia train/test THEO THỜI GIAN (80% đầu train, 20% cuối test).
   4. RandomForestRegressor cho 2 target: total_orders_1min & total_revenue_1min.
   5. Đánh giá RMSE / MAE / R2 + feature importance, lưu model joblib.

 Chạy:
   conda run -n crypto python ml/train_realtime_model.py
============================================================================
"""
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

load_dotenv()  # nạp .env (host, port, user, password ClickHouse)

MODEL_DIR = Path(__file__).resolve().parent / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9002"))
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "ecommerce")

LAG_FEATURES = ["total_orders_1min", "total_revenue_1min", "status_changed_count"]


def load_data() -> pd.DataFrame:
    client = Client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASSWORD, database=CH_DB)
    try:
        rows, cols = client.execute(
            """
            SELECT product_id, window_start,
                   total_orders_1min, total_revenue_1min,
                   avg_order_value_1min, status_changed_count
            FROM product_metrics_1min
            ORDER BY window_start
            """,
            with_column_types=True,
        )
    finally:
        client.disconnect()
    df = pd.DataFrame(rows, columns=[c[0] for c in cols])
    df["window_start"] = pd.to_datetime(df["window_start"])
    df = df.sort_values(["product_id", "window_start"]).reset_index(drop=True)
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    for col in LAG_FEATURES:
        for lag in (1, 2):
            df[f"{col}_lag{lag}"] = (
                df.groupby("product_id")[col].shift(lag).fillna(0)
            )
    df["hour"] = df["window_start"].dt.hour
    df["minute"] = df["window_start"].dt.minute
    return df


def evaluate(y_true, y_pred, name):
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred))
    print(f"  {name:<22} RMSE={rmse:10.3f}  MAE={mae:8.3f}  R2={r2:6.3f}")
    return rmse, mae, r2


def main():
    df = load_data()
    df = build_features(df)
    print(f"Rows: {len(df)} | products: {df['product_id'].nunique()} "
          f"| windows: {df['window_start'].nunique()} "
          f"({df['window_start'].min()} -> {df['window_start'].max()})")

    features = (
        ["hour", "minute"]
        + [f"{c}_lag1" for c in LAG_FEATURES]
        + [f"{c}_lag2" for c in LAG_FEATURES]
    )
    X = df[features].astype(float)
    targets = {"orders_1min": df["total_orders_1min"].astype(float),
               "revenue_1min": df["total_revenue_1min"].astype(float)}

    # Chia theo thời gian: 80% đầu train, 20% cuối test
    cut = df["window_start"].quantile(0.8)
    is_train = df["window_start"] <= cut
    X_tr, X_te = X[is_train], X[~is_train]
    print(f"Train {X_tr.shape[0]} rows | Test {X_te.shape[0]} rows "
          f"(cutoff {cut})")

    for name, y in targets.items():
        y_tr, y_te = y[is_train], y[~is_train]
        model = RandomForestRegressor(n_estimators=200, max_depth=8,
                                      random_state=42, n_jobs=-1)
        model.fit(X_tr, y_tr)
        pred = model.predict(X_te)
        print(f"\n== Target: {name} ==")
        evaluate(y_te.values, pred, "test")
        joblib.dump({"model": model, "features": features},
                    MODEL_DIR / f"product_{name}_model.joblib")
        print("  saved:", MODEL_DIR / f"product_{name}_model.joblib")

        # Feature importance
        imp = sorted(zip(features, model.feature_importances_),
                     key=lambda t: -t[1])[:5]
        print("  top features:", ", ".join(f"{f}={v:.3f}" for f, v in imp))

    # Sample: 3 sản phẩm có doanh thu lớn nhất ở cửa sổ cuối
    last = df["window_start"].max()
    tail = df[df["window_start"] == last].sort_values("total_revenue_1min",
                                                      ascending=False)
    print("\nSample data cửa sổ cuối", last)
    print(tail[["product_id", "total_orders_1min", "total_revenue_1min"]].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
