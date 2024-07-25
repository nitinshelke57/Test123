import psycopg2
import os

# Database connection details change here as
DB_NAME = "d6n8qu55o4imq5"
DB_USER = "u7vjhsqv3l9ejd"
DB_PASSWORD = "p50ba39301a742cde767fced8d8ff6d90017f3e2a62494a8ba13406eeb1b547e1"
DB_HOST = "ec2-54-84-224-250.compute-1.amazonaws.com"
DB_PORT = "5432"

# Directory to save JSON files
SAVE_DIR = "/qafiles/tests/test123"

# Ensure the save directory exists
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

def fetch_and_save_json():
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = connection.cursor()
        
        # Execute the query to fetch JSON data and package details
        cursor.execute("select package_export, package_name, package_id from integration_uat.tbl_vw_xplenty_support_pkg_info_20240701 where package_id in (157958)")
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Loop through each row and save the JSON data to a file
        for row in rows:
            json_data = row[0]
            package_name = row[1]
            package_id = row[2]
            file_name = f"{package_id}_{package_name}.json"
            file_path = os.path.join(SAVE_DIR, file_name)
            
            # Save the JSON data to a file as is
            with open(file_path, 'w') as json_file:
                json_file.write(json_data)
        
        print(f"Successfully saved {len(rows)} JSON records to {SAVE_DIR}")

    except Exception as error:
        print(f"Error: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

# Run the function to fetch and save JSON data
fetch_and_save_json()
