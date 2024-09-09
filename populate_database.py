import pandas as pd
import sqlite3

# Database connection
conn = sqlite3.connect('forage-walmart-task-4/shipment_database.db')
cursor = conn.cursor()

# Create tables if they do not exist
create_shipping_data_0_table = """
CREATE TABLE IF NOT EXISTS shipping_data_0 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_warehouse TEXT,
    destination_store TEXT,
    product TEXT,
    on_time INTEGER,
    product_quantity INTEGER,
    driver_identifier TEXT
);
"""
cursor.execute(create_shipping_data_0_table)

create_product_shipments_table = """
CREATE TABLE IF NOT EXISTS product_shipments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shipment_identifier TEXT,
    product TEXT,
    on_time INTEGER
);
"""
cursor.execute(create_product_shipments_table)

# Load the CSV files
spreadsheet_0 = pd.read_csv('forage-walmart-task-4/data/shipping_data_0.csv')
spreadsheet_1 = pd.read_csv('forage-walmart-task-4/data/shipping_data_1.csv')
spreadsheet_2 = pd.read_csv('forage-walmart-task-4/data/shipping_data_2.csv')

# Part 1: Insert Spreadsheet 0 directly into the database
def insert_spreadsheet_0():
    for _, row in spreadsheet_0.iterrows():
        cursor.execute(
            """
            INSERT INTO shipping_data_0 (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier) 
            VALUES (?, ?, ?, ?, ?, ?)""",
            (row['origin_warehouse'], row['destination_store'], row['product'], row['on_time'], row['product_quantity'], row['driver_identifier'])
        )
    conn.commit()

# Part 2: Combine Spreadsheets 1 and 2, and insert the result into the database
def process_spreadsheet_1_and_2():
    # Merge spreadsheet 1 and 2 on the 'shipment_identifier' column
    combined_data = pd.merge(spreadsheet_1, spreadsheet_2, on='shipment_identifier', how='inner')

    for _, row in combined_data.iterrows():
        cursor.execute(
            """
            INSERT INTO product_shipments (shipment_identifier, product, on_time) 
            VALUES (?, ?, ?)""",
            (row['shipment_identifier'], row['product'], row['on_time'])
        )
    conn.commit()

# Populate the database
insert_spreadsheet_0()
process_spreadsheet_1_and_2()

# Close the database connection
conn.close()

print("Data successfully populated into the database.")
