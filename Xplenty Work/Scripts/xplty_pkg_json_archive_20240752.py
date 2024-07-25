import psycopg2
import requests
from datetime import datetime

# Xplenty API details
XPLENTY_API_KEY = "4aAZPpUupNL7T4rVreU2yjZpBcFQ4Xwz"
XPLENTY_ACCOUNT_NAME = "zoetis-interface-uat"  # Example: "myaccount"

# Database connection details
DB_NAME = "d6n8qu55o4imq5"
DB_USER = "u7vjhsqv3l9ejd"
DB_PASSWORD = "p50ba39301a742cde767fced8d8ff6d90017f3e2a62494a8ba13406eeb1b547e1"
DB_HOST = "ec2-54-84-224-250.compute-1.amazonaws.com"
DB_PORT = "5432"

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
        cursor.execute("SELECT package_id, package_name FROM integration_uat.xplenty_redundant_packages_new WHERE json_archive_flg is null;")
        packages = cursor.fetchall()
        cursor.close()
        connection.close()
        return packages
    except Exception as error:
        print(f"Error fetching packages from database: {error}")
        return []

# Delete a package from Xplenty and update the archive flag
def delete_package(package_id, package_name):
    url = f"https://api.xplenty.com/{XPLENTY_ACCOUNT_NAME}/api/packages/{package_id}"
    headers = {
        'Accept': 'application/vnd.xplenty+json; version=2',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {XPLENTY_API_KEY}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        package_info = response.json()
        if package_info['status'] == 'archived':
            print(f"Package {package_name} (ID: {package_id}) is already archived.")
        else:
            response = requests.delete(url, headers=headers)
            if response.status_code == 200:
                print(f"Successfully deleted/archived package {package_name} (ID: {package_id})")
                update_archive_flag(package_id)
            else:
                print(f"Failed to delete/archive package {package_name} (ID: {package_id}): {response.status_code}, {response.text}")
    else:
        print(f"Failed to fetch package {package_name} (ID: {package_id}) details: {response.status_code}, {response.text}")

# Update the archive flag in the database
def update_archive_flag(package_id):
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
            "UPDATE integration_uat.xplenty_redundant_packages_new SET json_archive_flg = 'Y', json_archive_dt = %s WHERE package_id = %s",
            (current_timestamp, package_id)
        )
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Successfully updated archive flag for package ID: {package_id}")
    except Exception as error:
        print(f"Error updating archive flag in database: {error}")

# Main function
def main():
    packages = get_package_list()
    if not packages:
        print("No packages found.")
        return

    print("The following packages will be deleted:")
    for package_id, package_name in packages:
        print(f"Package Name: {package_name}, Package ID: {package_id}")

    confirmation = input("Are you sure you want to delete these packages? (yes/no): ")
    if confirmation.lower() == 'yes':
        for package_id, package_name in packages:
            delete_package(package_id, package_name)
    else:
        print("Deletion aborted.")

if __name__ == "__main__":
    main()
