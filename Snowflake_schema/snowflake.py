"""
This script loads sales-related CSV data into a PostgreSQL database
structured using a Snowflake schema.
"""

import os
import pandas as pd
import psycopg2

# 1. Connect to PostgreSQL database (use env variables in production)
conn = psycopg2.connect(
    host="localhost",
    database="store_snowflake_model",
    user="postgres",
    password="Luffy10$"
)
cursor = conn.cursor()

# 2. Load CSV files
sales_data = pd.read_csv('data/sales_data.csv')
customer_data = pd.read_csv('data/customer_data.csv')
product_data = pd.read_csv('data/product_data.csv')
location_data = pd.read_csv('data/location_data.csv')
date_data = pd.read_csv('data/date_data.csv')

# Function to get or insert and return ID
def get_or_insert(cursor, table, column, value):
    cursor.execute(
        f"SELECT {table[:-4]}_id FROM {table} WHERE {column} = %s",
        (value,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(
        f"INSERT INTO {table} ({column}) VALUES (%s) RETURNING {table[:-4]}_id",
        (value,)
    )
    return cursor.fetchone()[0]

# 3. Insert data into Product-related dimension tables
product_id_map = {}
for _, row in product_data.iterrows():
    category_id = get_or_insert(cursor, 'Category_Dim', 'category_name', row['category'])
    brand_id = get_or_insert(cursor, 'Brand_Dim', 'brand_name', row['brand'])
    cursor.execute(
        """INSERT INTO Product_Dim (product_name, category_id, brand_id) 
           VALUES (%s, %s, %s) RETURNING product_id""",
        (row['product_name'], category_id, brand_id)
    )
    product_id = cursor.fetchone()[0]
    product_id_map[row['product_name']] = product_id

# 4. Insert data into Location dimension tables
location_id_map = {}
for _, row in location_data.iterrows():
    city_id = get_or_insert(cursor, 'City_Dim', 'city_name', row['city'])
    state_id = get_or_insert(cursor, 'State_Dim', 'state_name', row['state'])
    country_id = get_or_insert(cursor, 'Country_Dim', 'country_name', row['country'])
    cursor.execute(
        """INSERT INTO Location_Dim (city_id, state_id, country_id) 
           VALUES (%s, %s, %s) RETURNING location_id""",
        (city_id, state_id, country_id)
    )
    loc_id = cursor.fetchone()[0]
    location_id_map[(row['city'], row['state'], row['country'])] = loc_id

# 5. Insert data into Customer dimension
customer_id_map = {}
for _, row in customer_data.iterrows():
    loc_id = location_id_map[(row['city'], row['state'], row['country'])]
    cursor.execute(
        """INSERT INTO Customer_Dim (customer_name, gender, age_group, location_id) 
           VALUES (%s, %s, %s, %s) RETURNING customer_id""",
        (row['customer_name'], row['gender'], row['age_group'], loc_id)
    )
    cust_id = cursor.fetchone()[0]
    customer_id_map[row['customer_name']] = cust_id

# 6. Insert data into Date dimension
date_id_map = {}
for _, row in date_data.iterrows():
    month_id = get_or_insert(cursor, 'Month_Dim', 'month_name', row['month'])
    cursor.execute(
        """INSERT INTO Date_Dim (year, quarter, month_id, day, weekday) 
           VALUES (%s, %s, %s, %s, %s) RETURNING date_id""",
        (row['year'], row['quarter'], month_id, row['day'], row['weekday'])
    )
    date_id = cursor.fetchone()[0]
    date_id_map[(row['year'], row['quarter'], row['month'], row['day'])] = date_id

# 7. Insert data into Fact table (Sales_Fact)
for _, row in sales_data.iterrows():
    customer_id = customer_id_map[row['customer_name']]
    product_id = product_id_map[row['product_name']]
    date_id = date_id_map[(row['year'], row['quarter'], row['month'], row['day'])]
    cursor.execute(
        """INSERT INTO Sales_Fact (customer_id, product_id, date_id, revenue, quantity) 
           VALUES (%s, %s, %s, %s, %s)""",
        (customer_id, product_id, date_id, row['revenue'], row['quantity'])
    )

# Commit all changes and close the connection
conn.commit()
cursor.close()
conn.close()
