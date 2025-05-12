SELECT l.city, l.state, l.country, SUM(s.sales_amount) AS total_sales
FROM Sales_Fact s
JOIN Location_Dim l ON s.location_id = l.location_id
GROUP BY l.city, l.state, l.country
ORDER BY total_sales DESC;
