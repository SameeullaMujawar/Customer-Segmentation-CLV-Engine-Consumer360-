use consumer360;
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(100),
    region VARCHAR(50)
);


INSERT INTO dim_customer (customer_name, region)
SELECT DISTINCT
    COALESCE(customer_name, 'Unknown'),
    COALESCE(region, 'Unknown')
FROM raw_sales;

