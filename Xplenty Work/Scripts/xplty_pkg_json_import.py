import psycopg2
import requests
import os
import json

# Xplenty API details change here
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
        cursor.execute("select package_name, package_id from integration_uat.tbl_vw_xplenty_support_pkg_info_20240701 where package_id in (157958);")
        packages = cursor.fetchall()
        cursor.close()
        connection.close()
        return packages
    except Exception as error:
        print(f"Error fetching packages from database: {error}")
        return []

# Upload a JSON file to Xplenty
def upload_package(json_file_path):
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
    else:
        print(f"Failed to upload package from {json_file_path}: {response.status_code}, {response.text}")

# Main function
def main():
    packages = get_package_list()
    if not packages:
        print("No packages found.")
        return

    for package_id, package_name in packages:
        json_file_name = f"{package_name}_{package_id}.json"
        json_file_path = os.path.join(JSON_DIR, json_file_name)
        if os.path.exists(json_file_path):
            upload_package(json_file_path)
        else:
            print(f"JSON file for package {package_name} (ID: {package_id}) not found at {json_file_path}")

if __name__ == "__main__":
    main()
