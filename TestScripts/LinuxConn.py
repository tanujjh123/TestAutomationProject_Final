import paramiko
import pandas as pd

# Setup SSH connection details
hostname = '192.168.0.104'  # Remote server's hostname or IP address
username = 'etlqalabs'  # SSH username
password = 'root'  # SSH password or use key-based authentication
remote_file_path = '/home/etlqalabs/sales_data.csv'  # Full path to the file on the remote server
local_file_path = 'TestData/sales_data_Linux.csv'  # Local path to save the file

try:
    # Establish SSH client connection
    ssh_client = paramiko.SSHClient()

    # Automatically add the server's SSH key (if not already known)
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect to the remote server
    ssh_client.connect(hostname, username=username, password=password)

    # Use SFTP to download the file from the remote server
    sftp = ssh_client.open_sftp()

    # Download the remote file to the local file path
    sftp.get(remote_file_path, local_file_path)
    print(f"File downloaded successfully from {remote_file_path} to {local_file_path}")

    # Close the SFTP session
    sftp.close()

    # Close the SSH client connection
    ssh_client.close()

    # Now read the downloaded file into a DataFrame
    df = pd.read_csv(local_file_path)

    # Display the first few rows of the DataFrame
    print("First few rows of the DataFrame:")
    print(df.head())

except Exception as e:
    print(f"An error occurred: {e}")
