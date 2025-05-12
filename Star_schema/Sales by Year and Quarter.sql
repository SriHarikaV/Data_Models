SELECT d.year, d.quarter, SUM(s.sales_amount) AS total_sales
FROM Sales_Fact s
JOIN Date_Dim d ON s.date_id = d.date_id
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;
