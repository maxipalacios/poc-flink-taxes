CREATE TABLE IF NOT EXISTS active_customers (
    id BIGINT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    balance NUMERIC(15, 2) NOT NULL,
    updated_at TIMESTAMPTZ
);
