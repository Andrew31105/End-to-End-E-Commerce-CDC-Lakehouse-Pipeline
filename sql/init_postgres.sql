-- ============================================================
-- PostgreSQL Initialization – Transactional E-Commerce Schema
-- ============================================================

-- 1) Danh mục sản phẩm
CREATE TABLE IF NOT EXISTS categories (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    description TEXT
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW() 
);

-- 2) Sản phẩm
CREATE TABLE IF NOT EXISTS products (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(255)   NOT NULL,
    category_id   INT            NOT NULL REFERENCES categories(id),
    base_price    NUMERIC(12, 2) NOT NULL CHECK (base_price >= 0),
    stock         INT            NOT NULL DEFAULT 0 CHECK (stock >= 0),
    is_active     BOOLEAN        NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- 3) Người dùng
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    full_name     VARCHAR(150)  NOT NULL,
    email         VARCHAR(255)  NOT NULL UNIQUE,
    phone         VARCHAR(20),
    city          VARCHAR(100),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW() 
);

-- 4) Đơn hàng – bảng chính cho CDC
CREATE TABLE IF NOT EXISTS orders (
    id            SERIAL PRIMARY KEY,
    user_id       INT            NOT NULL REFERENCES users(id),
    product_id    INT            NOT NULL REFERENCES products(id),
    quantity      INT            NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price    NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount  NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    status        VARCHAR(30)    NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','confirmed','shipped','delivered','cancelled','returned')),
    ordered_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Index hỗ trợ truy vấn CDC
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at  ON orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_user_id     ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_product_id  ON orders(product_id);

ALTER TABLE categories REPLICA IDENTITY DEFAULT;
ALTER TABLE products REPLICA IDENTITY DEFAULT;
ALTER TABLE users REPLICA IDENTITY DEFAULT;
ALTER TABLE orders REPLICA IDENTITY FULL;
-- ============================================================
-- Seed data mẫu (dữ liệu tĩnh ban đầu)
-- ============================================================

INSERT INTO categories (name, description) VALUES
    ('Điện thoại',      'Smartphone, feature phone'),
    ('Laptop',          'Laptop, notebook, ultrabook'),
    ('Phụ kiện',        'Tai nghe, sạc, cáp, ốp lưng'),
    ('Máy tính bảng',   'Tablet, iPad'),
    ('Thiết bị mạng',   'Router, switch, access point')
ON CONFLICT (name) DO NOTHING;

INSERT INTO products (name, category_id, base_price, stock) VALUES
    ('iPhone 15 Pro Max',       1, 29990000, 50),
    ('Samsung Galaxy S24 Ultra',1, 27990000, 40),
    ('MacBook Pro 14 M3',       2, 45990000, 20),
    ('Dell XPS 15',             2, 35990000, 15),
    ('AirPods Pro 2',           3,  5490000, 100),
    ('iPad Air M2',             4, 15990000, 30),
    ('Router WiFi 6 TP-Link',   5,  1890000, 80)
ON CONFLICT DO NOTHING;

INSERT INTO users (full_name, email, phone, city) VALUES
    ('Nguyễn Văn A',  'nguyenvana@example.com',  '0901000001', 'Hà Nội'),
    ('Trần Thị B',    'tranthib@example.com',    '0901000002', 'TP.HCM'),
    ('Lê Văn C',      'levanc@example.com',      '0901000003', 'Đà Nẵng'),
    ('Phạm Thị D',    'phamthid@example.com',    '0901000004', 'Hải Phòng'),
    ('Hoàng Văn E',   'hoangvane@example.com',   '0901000005', 'Cần Thơ')
ON CONFLICT DO NOTHING;

-- Một vài đơn hàng mẫu ban đầu
INSERT INTO orders (user_id, product_id, quantity, unit_price, total_amount, status) VALUES
    (1, 1, 1, 29990000, 29990000, 'delivered'),
    (2, 3, 1, 45990000, 45990000, 'shipped'),
    (3, 5, 2,  5490000, 10980000, 'pending'),
    (4, 2, 1, 27990000, 27990000, 'confirmed'),
    (5, 7, 3,  1890000,  5670000, 'pending')
ON CONFLICT DO NOTHING;
