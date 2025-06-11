
from datetime import datetime
import logging
import paramiko
import os
import gzip
import psycopg2
import pytz
import shutil
import socket
import xml.etree.ElementTree as ET
from time import sleep


"""
docker run --name kpiairscale -e POSTGRES_USER=Solis -e POSTGRES_PASSWORD=Solis2025 -e POSTGRES_DB=kpiairscale -p 5433:5432 -d postgres:11
docker run --name kpiAirScale -e POSTGRES_USER=Solis -e POSTGRES_PASSWORD=Solis2025 -e POSTGRES_DB=kpiAirScale -d -p 5436:5432 -v pgdata:/Userdata
/postgresql/data postgres:11
"""


# List of radios with their connection details
radios = [
    {"server_ip": "10.1.16.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    {"server_ip": "10.1.16.21","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    {"server_ip": "10.1.38.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    {"server_ip": "10.1.39.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    {"server_ip": "10.1.42.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    {"server_ip": "10.1.50.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    {"server_ip": "10.1.52.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    {"server_ip": "10.1.53.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    {"server_ip": "10.1.8.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
]

# Local directory where files will be saved
# dir_zip = r"/var/openkpi/kpi_zip"
# dir_files = r"/var/openkpi/kpi_files"
# dir_files_read = r"/var/openkpi/kpi_files_read"
dir_zip = r'/Userdata/proj2025/parsingkpi/kpi_zip'
dir_files = r'/Userdata/proj2025/parsingkpi/kpi_files'
dir_files_read = r'/Userdata/proj2025/parsingkpi/kpi_files_read'


# dir_zip = r"/var/openkpi/kpi_files"
os.makedirs(dir_zip, exist_ok=True)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("kpiAirScale.log"),
        logging.StreamHandler()
    ]
)

# PostgreSQL connection config
# db_config = {
#     "dbname": os.getenv("kpiAirScale"),
#     "user": os.getenv("Solis"),
#     "password": os.getenv("Solis2025"),
#     "host": os.getenv("localhost"),
#     "port": int(os.getenv("DB_PORT", 5436))
# }
db_config = {
    "dbname": "kpiAirScale",
    "user": "Solis",
    "password": "Solis2025",
    "host": "localhost",
    "port": 5436
}

def adjust_file_name(original_name):
    """
    Adjust the file name to change the extension from .raw to .xml.
    Example: "PM.BTS-414225.20250505.151500.LTE.raw" -> "PM.BTS-414225.20250505.151500.LTE.xml"
    """
    if original_name.endswith(".raw.gz"):
        new_name = original_name.replace("0000.ANY.raw.gz", "0000.xml")
        return new_name, None  # No need to return a timestamp
    return original_name, None  # Return the original name if it doesn't end with .raw

def download_files():
    for radio in radios:
        max_retries = 1
        retry_delay = 3  # seconds between retries
        connected = False

        for attempt in range(max_retries):
            server_ip = radio["server_ip"]
            username = radio["username"]
            password = radio["password"]
            remote_directory = radio["remote_directory"]
            
            transport = None
            sftp = None

            try:
                logging.info(f"Attempt {attempt + 1}/{max_retries} to connect to {server_ip}")

                # Establish SFTP connection
                transport = paramiko.Transport((server_ip, 22))
                transport.connect(username=username, password=password)

                # Create the SFTP client
                sftp = paramiko.SFTPClient.from_transport(transport)

                # List files in the remote directory
                logging.info(f"Connecting to server {server_ip}...")
                remote_files = sftp.listdir(remote_directory)

                for file_name in remote_files:
                    # Check if the file matches the quarterly KPI naming pattern
                    if file_name.startswith("PM.BTS") and file_name.endswith("0000.ANY.raw.gz"):
                        # Adjust the file name and extract the timestamp
                        new_file_name, _ = adjust_file_name(file_name)

                        # Define remote and local file paths
                        remote_file_path = os.path.join(remote_directory, file_name)
                        local_file_zip = os.path.join(dir_zip, new_file_name)
                        local_file_path = os.path.join(dir_files, new_file_name)

                        # Check if the file already exists locally
                        if os.path.exists(local_file_zip):
                            # print(f"File {new_file_name} already exists locally. Skipping download.")
                            continue

                        # Download the file with the new name
                        # logging.info(f"Downloading {file_name} as {new_file_name}...")
                        sftp.get(remote_file_path, local_file_zip)
                        logging.info(f"File {new_file_name} downloaded successfully.")

                        # Open the .gz file and write the uncompressed data to the output file
                        #print(local_file_zip)
                        with gzip.open(local_file_zip, 'rb') as f_in:
                            with open(local_file_path, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)

                        try:
                            for filename in os.listdir(dir_zip):
                                file_path = os.path.join(dir_zip, filename)
                                if os.path.isfile(file_path):
                                    os.remove(file_path)
                                    print(f'Removed: {file_path}')
                        except PermissionError as e:
                            logging.error(f'Error: {e}')
                        except Exception as e:
                            logging.error(f'An error occurred: {e}')

                connected = True
                break  # Success, exit retry loop

            except paramiko.SSHException as e:
                logging.error(f"SSH error connecting to {server_ip}: {str(e)}")
            except socket.timeout:
                logging.error(f"Connection timeout for {server_ip}")
            except socket.error as e:
                logging.error(f"Socket error for {server_ip}: {str(e)}")
            except Exception as e:
                logging.error(f"Unexpected error with {server_ip}: {str(e)}")  

            finally:
                if sftp: sftp.close()
                if transport: transport.close()
                
            if attempt < max_retries - 1:
                # logging.info(f"Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

        if not connected:
            logging.error(f"Failed to connect to {server_ip} after {max_retries} attempts")
            continue  # Skip to next radio
            
        # Continue with file processing if connection was successful
        logging.info(f"Successfully processed files from {server_ip}")


def is_empty_kpigroup(measurementType, conn):
    """
    Check if the measurementType exists in the empty_kpigroup table.
    Returns True if it exists, False otherwise.
    """
    cursor = conn.cursor()
    cursor.execute(
        'SELECT 1 FROM "Empty_kpiGroup" WHERE kpigroup = %s LIMIT 1;',
        (measurementType,)
    )
    exists = cursor.fetchone() is not None
    cursor.close()
    
    return bool(exists)


def create_table_if_not_exists(measurementType, kpi_columns):
    conn = psycopg2.connect(**db_config)
    if is_empty_kpigroup(measurementType, conn):
        conn.close()
        return
    
    cursor = conn.cursor()
    columns = ', '.join([f'"{col}" INTEGER' for col in kpi_columns])
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS "{measurementType}" (
        id SERIAL PRIMARY KEY,
        create_at TIMESTAMP,
        manage_object TEXT,
        {columns}
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def insert_into_table(measurementType, data, kpi_columns):
    conn = psycopg2.connect(**db_config)

    if is_empty_kpigroup(measurementType, conn):
        conn.close()
        return        

    cursor = conn.cursor()
    columns = ', '.join([f'"{col}"' for col in kpi_columns])
    placeholders = ', '.join(['%s'] * (len(kpi_columns) + 2))  # +2 for create_at and manage_object
    insert_query = f'''
    INSERT INTO "{measurementType}" (create_at, manage_object, {columns})
    VALUES ({placeholders})
    '''
    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()


def process_kpi_file(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    logging.info(f"Processing file: {file_path}")

    # Initialize a dictionary to store data for each measurementType
    for PMSetup in root.iter('PMSetup'):
        createAt = PMSetup.attrib.get('startTime')
        for PMMOResult in PMSetup.iter('PMMOResult'):
            manageObject = None
            for child in PMMOResult:
                if child.tag == 'MO' and child.attrib.get('dimension') == 'network_element':
                    for subchild in child:
                        if subchild.tag == 'DN':
                            original = subchild.text
                            manageObject = original.replace("PLMN-PLMN/", "")  # Replace spaces with underscores
                elif child.tag == 'NE-WBTS_1.0':
                    measurementType = child.attrib.get('measurementType')
                    kpi_dict = {}
                    for subchild in child:
                        try:
                            value = int(subchild.text)
                        except (TypeError, ValueError):
                            continue
                        # if value != 0:
                        kpi_dict[subchild.tag] = value

                    if kpi_dict:  # Only proceed if there are non-zero kpiValues
                        kpi_columns = sorted(kpi_dict.keys())
                        create_table_if_not_exists(measurementType, kpi_columns)
                        row = [createAt, manageObject] + [kpi_dict.get(col) for col in kpi_columns]
                        insert_into_table(measurementType, [row], kpi_columns)

    # Move the file to the read directory
    dst_path = os.path.join(dir_files_read, os.path.basename(file_path))
    shutil.move(file_path, dst_path)  # Move the file to the read directory

# Loop through all files in the directory
def process_all_files():
    files = sorted(os.listdir(dir_files))
    for file_name in files:
        file_path = os.path.join(dir_files, file_name)
        if os.path.isfile(file_path):  # Ensure it's a file
            process_kpi_file(file_path)

# Main Execution
if __name__ == "__main__":
    # download_files()
    process_all_files()