import psycopg2
import os
from datetime import datetime

# Database connection details
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
        cursor.execute("SELECT package_json_export, package_name, package_id, pkg_last_execution_date, pkg_analysis, json_bkp_flg, json_bkp_dt FROM integration_uat.xplenty_redundant_packages_new WHERE json_bkp_flg IS NULL AND json_archive_flg IS NULL")
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Display the packages that will be saved
        print("The following packages will be saved:")
        for row in rows:
            print(f"Package Name: {row[1]}, Last Execution Date: {row[3]}, Analysis: {row[4]}")
        
        # Ask for confirmation
        confirm = input("Do you want to proceed with saving these packages? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Operation cancelled.")
            return
        
        # Loop through each row and save the JSON data to a file
        for row in rows:
            json_data = row[0]
            package_name = row[1]
            package_id = row[2]
            file_name = f"{package_id}_{package_name}.json"
            file_path = os.path.join(SAVE_DIR, file_name)
            
            # Save the JSON data to a file
            with open(file_path, 'w') as json_file:
                json_file.write(json_data)
            
            # Update the database with the current timestamp, file path, and file name
            current_timestamp = datetime.now()
            cursor.execute(
                "UPDATE integration_uat.xplenty_redundant_packages_new SET json_bkp_flg = 'Y', json_bkp_dt = %s, json_bkp_path = %s, json_bkp_file_nm = %s WHERE package_id = %s",
                (current_timestamp, SAVE_DIR, file_name, package_id)
            )
        
        # Commit the transaction
        connection.commit()
        
        print(f"Successfully saved {len(rows)} JSON records to {SAVE_DIR} and updated the database.")

    except Exception as error:
        print(f"Error: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

# Run the function to fetch and save JSON data
fetch_and_save_json()
