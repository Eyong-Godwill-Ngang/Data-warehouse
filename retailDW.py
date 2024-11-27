import mysql.connector
import pandas as pd

# Paths to your CSV files
customer_csv = 'Customer_Dim.csv'
product_csv = 'Product_Dim.csv'
date_csv = 'Date_Dim.csv'
sales_csv = 'Sales_Fact.csv'

# Load CSVs and remove duplicates based on unique IDs
customer_df = pd.read_csv(customer_csv).drop_duplicates(subset=['customer_id'])
product_df = pd.read_csv(product_csv).drop_duplicates(subset=['product_id'])
date_df = pd.read_csv(date_csv).drop_duplicates(subset=['date_id'])
sales_df = pd.read_csv(sales_csv).drop_duplicates(subset=['sale_id'])

try:
    # Database connection details
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='retaildw'
    )
    cursor = conn.cursor()

    # Insert data into Customer_Dim
    for _, row in customer_df.iterrows():
        cursor.execute("""
  INSERT INTO Customer_Dim (customer_id, customer_name, city, age, gender)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['customer_id'], row['customer_name'], row['city'], row['age'], row['gender']))

    # Insert data into Product_Dim
    for _, row in product_df.iterrows():
        cursor.execute("""
            INSERT INTO Product_Dim (product_id, product_name, category, price)
            VALUES (%s, %s, %s, %s)
        """, (row['product_id'], row['product_name'], row['category'], row['price']))

    # Insert data into Date_Dim
    for _, row in date_df.iterrows():
        cursor.execute("""
            INSERT INTO Date_Dim (date_id, date, day_of_week, month, year)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['date_id'], row['date'], row['day_of_week'], row['month'], row['year']))

    # Insert data into Sales_Fact
    for _, row in sales_df.iterrows():
        cursor.execute("""
            INSERT INTO Sales_Fact (sale_id, customer_id, product_id, date_id, quantity, total_amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row['sale_id'], row['customer_id'], row['product_id'], row['date_id'], row['quantity'], row['total_amount']))

    # Commit the transactions
    conn.commit()

    # Database connection details
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='retaildw'
    )
    cursor = conn.cursor()

    # Insert data into Customer_Dim
    for _, row in customer_df.iterrows():
        cursor.execute("""
            INSERT INTO Customer_Dim (customer_id, customer_name, city, age, gender)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['customer_id'], row['customer_name'], row['city'], row['age'], row['gender']))

    # Insert data into Product_Dim
    for _, row in product_df.iterrows():
        cursor.execute("""
            INSERT INTO Product_Dim (product_id, product_name, category, price)
            VALUES (%s, %s, %s, %s)
        """, (row['product_id'], row['product_name'], row['category'], row['price']))

    # Insert data into Date_Dim
    for _, row in date_df.iterrows():
        cursor.execute("""
            INSERT INTO Date_Dim (date_id, date, day_of_week, month, year)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['date_id'], row['date'], row['day_of_week'], row['month'], row['year']))

    # Insert data into Sales_Fact
    for _, row in sales_df.iterrows():
        cursor.execute("""
            INSERT INTO Sales_Fact (sale_id, customer_id, product_id, date_id, quantity, total_amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row['sale_id'], row['customer_id'], row['product_id'], row['date_id'], row['quantity'], row['total_amount']))

  # Commit the transactions
    conn.commit()
    print("Data inserted successfully.")

except mysql.connector.Error as e:
    print(f"Error while connecting to MySQL: {e}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection is closed.")
