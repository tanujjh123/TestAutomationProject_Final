# config.py

# MySQL Database Configuration
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3306'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'Omnitech%407329'
MYSQL_DATABASE = 'retaildwh'

# Oracle Database Configuration
ORACLE_USER = 'system'
ORACLE_PASSWORD = 'admin'
ORACLE_HOST = 'localhost'
ORACLE_PORT = '1521'
ORACLE_SERVICE ='xe'

# Linux Setup SSH connection details
hostname = '192.168.0.104'  # Remote server's hostname or IP address
username = 'etlqalabs'  # SSH username
password = 'root'  # SSH password or use key-based authentication
remote_file_path = '/home/etlqalabs/sales_data.csv'  # Full path to the file on the remote server
local_file_path = 'TestData/sales_data_Linux.csv'  # Local path to save the file
