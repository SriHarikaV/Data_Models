import pandas as pd
import psycopg2

# 1. Connect to PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="store_model",
    user="postgres",
    password="Luffy10$"
)
cursor = conn.cursor()

# 2. Load data from CSV files
sales_data = pd.read_csv('data/sales_data.csv')
customer_data = pd.read_csv('data/customer_data.csv')
product_data = pd.read_csv('data/product_data.csv')
location_data = pd.read_csv('data/location_data.csv')
date_data = pd.read_csv('data/date_data.csv')

# 3. Insert Data into Dimension Tables
for _, row in product_data.iterrows():
    cursor.execute("""
        INSERT INTO Product_Dim (product_name, category, brand) 
        VALUES (%s, %s, %s)
    """, (row['product_name'], row['category'], row['brand']))

for _, row in location_data.iterrows():
    cursor.execute("""
        INSERT INTO Location_Dim (city, state, country) 
        VALUES (%s, %s, %s)
    """, (row['city'], row['state'], row['country']))

for _, row in customer_data.iterrows():
    cursor.execute("""
        INSERT INTO Customer_Dim (customer_name, gender, age_group, location_id) 
        VALUES (%s, %s, %s, %s)
    """, (row['customer_name'], row['gender'], row['age_group'], row['location_id']))

for _, row in date_data.iterrows():
    cursor.execute("""
        INSERT INTO Date_Dim (year, quarter, month, day, weekday) 
        VALUES (%s, %s, %s, %s, %s)
    """, (row['year'], row['quarter'], row['month'], row['day'], row['weekday']))

# 4. Insert Data into Fact Table
for _, row in sales_data.iterrows():
    cursor.execute("""
        INSERT INTO Sales_Fact (product_id, customer_id, date_id, location_id, sales_amount, quantity_sold) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (row['product_id'], row['customer_id'], row['date_id'], row['location_id'], row['sales_amount'], row['quantity_sold']))

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()
