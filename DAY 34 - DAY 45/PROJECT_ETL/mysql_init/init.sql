-- Use the ecommerce database
USE ecommerce_db;

-- Create products table
CREATE TABLE IF NOT EXISTS products (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  price DECIMAL(10, 2),
  category VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE,
  address TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create orders table with foreign key constraints
CREATE TABLE IF NOT EXISTS orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT,
  product_id INT,
  quantity INT,
  total_price DECIMAL(10, 2),
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
  FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- Insert data into products
INSERT INTO products (name, price, category) VALUES
('Apple iPhone 14', 799.99, 'Electronics'),
('Samsung Galaxy S22', 699.99, 'Electronics'),
('Sony WH-1000XM4 Headphones', 299.99, 'Electronics'),
('Dell XPS 13 Laptop', 1099.49, 'Computers'),
('Asus ROG Gaming Mouse', 49.99, 'Computers'),
('Nike Air Max 270', 129.99, 'Footwear'),
('Adidas Ultraboost', 139.95, 'Footwear'),
('Levi\'s Men\'s Jeans', 59.90, 'Clothing'),
('H&M Cotton T-Shirt', 14.99, 'Clothing'),
('KitchenAid Stand Mixer', 349.00, 'Home Appliances'),
('Philips LED Bulb Pack', 19.99, 'Home Appliances'),
('The Psychology of Money', 16.99, 'Books');

-- Insert data into customers
INSERT INTO customers (name, email, address) VALUES
('Alice Johnson', 'alice.johnson@example.com', '123 Maple Street, Springfield'),
('Bob Smith', 'bob.smith@example.com', '456 Oak Avenue, Metropolis'),
('Charlie Lee', 'charlie.lee@example.com', '789 Pine Lane, Gotham'),
('Diana Prince', 'diana.prince@example.com', '321 Hero Blvd, Themyscira'),
('Ethan Hunt', 'ethan.hunt@example.com', '987 Mission Rd, Langley'),
('Fiona Gallagher', 'fiona.g@example.com', '654 South Side, Chicago'),
('George Martin', 'george.m@example.com', '1357 Elm St, Riverdale'),
('Hannah Baker', 'hannah.b@example.com', '753 Willow Dr, Monterey'),
('Ian Malcolm', 'ian.m@example.com', '852 Chaos Way, Isla Nublar'),
('Jane Foster', 'jane.foster@example.com', '951 Astrophysics St, New York');

-- Insert data into orders (foreign keys must match existing customer and product IDs)
INSERT INTO orders (customer_id, product_id, quantity, total_price) VALUES
(1, 1, 1, 799.99),
(2, 3, 2, 599.98),
(3, 5, 1, 49.99),
(4, 4, 1, 1099.49),
(5, 2, 1, 699.99),
(6, 7, 1, 139.95),
(7, 9, 3, 44.97),
(8, 11, 2, 39.98),
(9, 6, 1, 129.99),
(10, 8, 1, 59.90),
(1, 12, 2, 33.98),
(2, 10, 1, 349.00);
