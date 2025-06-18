-- Set schema for ecommerce_warehouse
\c ecommerce_warehouse

-- Create schemas
CREATE SCHEMA IF NOT EXISTS dimensions;
CREATE SCHEMA IF NOT EXISTS facts;

-- ================================
-- Dimension Table: Products
-- ================================
CREATE TABLE IF NOT EXISTS dimensions.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2),
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Orders Dimension (if needed for denormalized reporting or history tracking)
CREATE TABLE IF NOT EXISTS dimensions.orders (
    id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10, 2),
    order_date TIMESTAMP,
    last_updated TIMESTAMP
);

-- ================================
-- Dimension Table: Customers
-- ================================
CREATE TABLE IF NOT EXISTS dimensions.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================
-- Fact Table: Orders
-- ================================
CREATE TABLE IF NOT EXISTS facts.orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dimensions.customers(id),
    product_id INT REFERENCES dimensions.products(id),
    quantity INT,
    total_price DECIMAL(10, 2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Customers Fact (Aggregated customer purchase summary)
CREATE TABLE IF NOT EXISTS facts.customers (
    customer_id INT,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    total_orders INT,
    total_spent DECIMAL(12, 2),
    report_date DATE
);

-- ================================
-- Fact Table: Products Summary
-- (Used by Spark job in Airflow DAG)
-- ================================
CREATE TABLE IF NOT EXISTS facts.products (
    product_name VARCHAR(255),
    category VARCHAR(100),
    total_quantity INT,
    total_revenue DECIMAL(12, 2),
    report_date DATE
);
