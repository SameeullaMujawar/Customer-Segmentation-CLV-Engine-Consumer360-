#Revenue Trend
SELECT order_date, SUM(total_amount) AS revenue
FROM fact_sales
GROUP BY order_date
ORDER BY order_date;

#Top Products
SELECT p.product_name, SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC;

#Customer Summary
SELECT
    customer_id,
    COUNT(*) AS frequency,
    SUM(total_amount) AS monetary,
    MAX(order_date) AS last_purchase
FROM fact_sales
GROUP BY customer_id;


