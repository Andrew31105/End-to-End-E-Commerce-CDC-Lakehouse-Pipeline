# ML demo (Sprint 4)

Dùng feature real-time từ ClickHouse (`ecommerce.product_metrics_1min`, do
Flink per-product sinh ra) để train model dự đoán **quy mô đơn / doanh thu của
sản phẩm trong 1 phút tới**.

## Các bước

```bash
# 1) Train (đọc ClickHouse, feature lag 1-2 phút + hour/minute,
#          chia theo thời gian 80/20, RandomForest, lưu model joblib)
conda run -n crypto python ml/train_realtime_model.py

# 2) Online inference demo (gọi Feature Store API :8000 + ClickHouse -> predict)
conda run -n crypto python ml/predict_next.py --product-id 3
```

## Vì sao ý nghĩa
- Thể hiện **Serving Layer → ML**: model train/đọc feature từ ClickHouse/Flink,
  inference dùng đúng endpoint Feature Store (`GET /features/order_features`).
- Data thật: ~463 dòng × 7 sản phẩm × ~68 phút (12:51–14:08 UTC+7).
- Lưu ý demo: model dự đoán theo quy luật chung (không dùng product_id) nên có
  thể áp cho sản phẩm mới; data còn ít & theo event-window nên RMSE/R2 chỉ để
  minh hoạ pipeline, không phải model sản xuất.
