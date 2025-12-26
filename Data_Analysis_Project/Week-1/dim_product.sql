DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50)
);
