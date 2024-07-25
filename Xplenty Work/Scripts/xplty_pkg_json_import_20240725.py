import psycopg2
import requests
import os
import json
from datetime import datetime

# Xplenty API details
XPLENTY_API_KEY = "4aAZPpUupNL7T4rVreU2yjZpBcFQ4Xwz"
XPLENTY_ACCOUNT_NAME = "zoetis-interface-uat"

# Database connection details
DB_NAME = "d6n8qu55o4imq5"
DB_USER = "u7vjhsqv3l9ejd"
DB_PASSWORD = "p50ba39301a742cde767fced8d8ff6d90017f3e2a62494a8ba13406eeb1b547e1"
DB_HOST = "ec2-54-84-224-250.compute-1.amazonaws.com"
DB_PORT = "5432"

# Directory containing JSON files
JSON_DIR = "/qafiles/tests/test123"

# Connect to PostgreSQL database
def get_package_list():
    try:
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = connection.cursor()
        cursor.execute("SELECT package_id, package_name FROM integration_uat.xplenty_redundant_packages_new WHERE json_import_flg = 'Y';")
        packages = cursor.fetchall()
        cursor.close()
        connection.close()
        return packages
    except Exception as error:
        print(f"Error fetching packages from database: {error}")
        return []

# Upload a JSON file to Xplenty
def upload_package(json_file_path, package_id):
    url = f"https://api.xplenty.com/{XPLENTY_ACCOUNT_NAME}/api/packages"
    headers = {
        'Accept': 'application/vnd.xplenty+json; version=2',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {XPLENTY_API_KEY}'
    }
    
    with open(json_file_path, 'r') as json_file:
        package_data = json.load(json_file)

    response = requests.post(url, headers=headers, json=package_data)
    if response.status_code == 201:
        print(f"Successfully uploaded package from {json_file_path}")
        update_import_flag(package_id)
    else:
        print(f"Failed to upload package from {json_file_path}: {response.status_code}, {response.text}")

# Update the import flag in the database
def update_import_flag(package_id):
    try:
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = connection.cursor()
        current_timestamp = datetime.now()
        cursor.execute(
            "UPDATE integration_uat.xplenty_redundant_packages_new SET json_import_flg = null, json_import_dt = %s WHERE package_id = %s",
            (current_timestamp, package_id)
        )
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Successfully updated import flag for package ID: {package_id}")
    except Exception as error:
        print(f"Error updating import flag in database: {error}")

# Main function
def main():
    packages = get_package_list()
    if not packages:
        print("No packages found.")
        return

    print("The following packages will be imported:")
    for package_id, package_name in packages:
        print(f"Package Name: {package_name}, Package ID: {package_id}")

    confirmation = input("Are you sure you want to import these packages? (yes/no): ")
    if confirmation.lower() == 'yes':
        for package_id, package_name in packages:
            json_file_name = f"{package_id}_{package_name}.json"
            json_file_path = os.path.join(JSON_DIR, json_file_name)
            if os.path.exists(json_file_path):
                upload_package(json_file_path, package_id)
            else:
                print(f"JSON file for package {package_name} (ID: {package_id}) not found at {json_file_path}")
    else:
        print("Import aborted.")

if __name__ == "__main__":
    main()
