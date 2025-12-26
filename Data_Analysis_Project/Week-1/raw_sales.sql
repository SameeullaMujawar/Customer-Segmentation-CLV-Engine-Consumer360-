DROP TABLE IF EXISTS raw_sales;

CREATE TABLE raw_sales (
    order_id VARCHAR(50),
    order_date VARCHAR(50),
    customer_name VARCHAR(100),
    region VARCHAR(50),
    sales_person VARCHAR(100),
    product_category VARCHAR(50),
    product_name VARCHAR(100),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(5,2),
    revenue DECIMAL(10,2),
    payment_method VARCHAR(50),
    delivery_status VARCHAR(50)
);
