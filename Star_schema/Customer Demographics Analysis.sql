SELECT c.gender, c.age_group, SUM(s.sales_amount) AS total_sales
FROM Sales_Fact s
JOIN Customer_Dim c ON s.customer_id = c.customer_id
GROUP BY c.gender, c.age_group
ORDER BY total_sales DESC;
