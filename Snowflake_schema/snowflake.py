import pandas as pd
import psycopg2

# 1. Connect to PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="store_snowflake_model",
    user="postgres",
    password="Luffy10$"
)
cursor = conn.cursor()

# Load data from CSV files
sales_data = pd.read_csv('data/sales_data.csv')
customer_data = pd.read_csv('data/customer_data.csv')
product_data = pd.read_csv('data/product_data.csv')
location_data = pd.read_csv('data/location_data.csv')
date_data = pd.read_csv('data/date_data.csv')

# Function to get or insert and get ID
def get_or_insert(cursor, table, column, value):
    cursor.execute(f"SELECT {table[:-4]}_id FROM {table} WHERE {column} = %s", (value,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(f"INSERT INTO {table} ({column}) VALUES (%s) RETURNING {table[:-4]}_id", (value,))
    return cursor.fetchone()[0]

# Insert Data into Dimension Tables
product_id_map = {}
for _, row in product_data.iterrows():
    category_id = get_or_insert(cursor, 'Category_Dim', 'category_name', row['category'])
    brand_id = get_or_insert(cursor, 'Brand_Dim', 'brand_name', row['brand'])
    cursor.execute("""
        INSERT INTO Product_Dim (product_name, category_id, brand_id) 
        VALUES (%s, %s, %s) RETURNING product_id
    """, (row['product_name'], category_id, brand_id))
    product_id = cursor.fetchone()[0]
    product_id_map[row['product_name']] = product_id

location_id_map = {}
for _, row in location_data.iterrows():
    city_id = get_or_insert(cursor, 'City_Dim', 'city_name', row['city'])
    state_id = get_or_insert(cursor, 'State_Dim', 'state_name', row['state'])
    country_id = get_or_insert(cursor, 'Country_Dim', 'country_name', row['country'])
    cursor.execute("""
        INSERT INTO Location_Dim (city_id, state_id, country_id) 
        VALUES (%s, %s, %s) RETURNING location_id
    """, (city_id, state_id, country_id))
    location_id = cursor.fetchone()[0]
    location_id_map[(row['city'], row['state'], row['country'])] = location_id

customer_id_map = {}
for _, row in customer_data.iterrows():
    location_id = location_id_map[(row['city'], row['state'], row['country'])]
    cursor.execute("""
        INSERT INTO Customer_Dim (customer_name, gender, age_group, location_id) 
        VALUES (%s, %s, %s, %s) RETURNING customer_id
    """, (row['customer_name'], row['gender'], row['age_group'], location_id))
    customer_id = cursor.fetchone()[0]
    customer_id_map[row['customer_name']] = customer_id

date_id_map = {}
for _, row in date_data.iterrows():
    month_id = get_or_insert(cursor, 'Month_Dim', 'month_name', row['month'])
    cursor.execute("""
        INSERT INTO Date_Dim (year, quarter, month_id, day, weekday) 
        VALUES (%s, %s, %s, %s, %s) RETURNING date_id
    """, (row['year'], row['quarter'], month_id, row['day'], row['weekday']))
    date_id = cursor.fetchone()[0]
    date_id_map[(row['year'], row['quarter'], row['month'], row['day'])] = date_id

# Insert Data into Fact Table
for _, row in sales_data.iterrows():
    product_id = product_id_map[row['product_name']]
    customer_id = customer_id_map[row['customer_name']]
    date_id = date_id_map[(row['year'], row['quarter'], row['month'], row['day'])]
    location_id = location_id_map[(row['city'], row['state'], row['country'])]
    cursor.execute("""
        INSERT INTO Sales_Fact (product_id, customer_id, date_id, location_id, sales_amount, quantity_sold) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (product_id, customer_id, date_id, location_id, row['sales_amount'], row['quantity_sold']))

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()
