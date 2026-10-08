#!/usr/bin/env python3
"""
============================================================================
 scripts/db.py
 Tiện ích PostgreSQL dùng chung cho các script chạy trên máy host
 (mock_data.py, olist_load_postgres.py, ...)
============================================================================
 Nạp cấu hình kết nối theo thứ tự ưu tiên:
   POSTGRES_*  →  fallback  PG_*
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_env_file(path: Path | None = None) -> None:
    """Nạp biến từ file .env vào os.environ (không ghi đè biến đã set).

    Mặc định tìm theo thứ tự: ./.env  →  <project_root>/.env
    """
    candidates = [path] if path else [Path.cwd() / ".env", PROJECT_ROOT / ".env"]
    for candidate in candidates:
        if not candidate or not candidate.is_file():
            continue
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return


def db_config() -> dict[str, Any]:
    """Trả về kwargs kết nối psycopg2 từ biến môi trường (.env)."""
    load_env_file()
    config: dict[str, Any] = {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", os.getenv("PG_DB") or "ecommerce"),
        "user": os.getenv("POSTGRES_USER", os.getenv("PG_USER")),
        "password": os.getenv("POSTGRES_PASSWORD", os.getenv("PG_PASSWORD")),
    }
    if not config["user"] or not config["password"]:
        raise SystemExit(
            "Thiếu thông tin đăng nhập PostgreSQL. "
            "Hãy tạo file .env từ .env.example rồi chạy lại."
        )
    return config


def connect(config: dict[str, Any] | None = None):
    """Mở kết nối psycopg2 tới PostgreSQL (autocommit tắt)."""
    import psycopg2

    return psycopg2.connect(**(config or db_config()))
