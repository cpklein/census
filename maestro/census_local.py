#Local Environment variables
import socket
import os

KNOWN_EXTENSIONS = ['.json', '.csv', '.parquet', '.zip']
CHUNK_SIZE = 1024
census_id = socket.gethostname() 

# development environment
if census_id in ['1125-CKLEIN.local']:
    census_dir = '/Users/caioklein/Development/census'
    data_dir = '/Users/caioklein/Development/data'
elif census_id in ['german_shepherd']:
    census_dir = '/Users/caio/Development/integra/census'
    data_dir = '/Users/caio/Development/integra/data'
# production environment
else:
    census_dir = '/home/ubuntu/census'
    data_dir = '/data/census'

# syslog server    
syslog_server = "ec2-3-239-79-152.compute-1.amazonaws.com"

# File Configuration on package
meltano_dir = os.path.join(census_dir, 'meltano')
meltano_log_config_file = os.path.join(meltano_dir, 'logging_template.yaml')

# File Configuration data
db_dir = os.path.join(data_dir, 'database')
log_dir = os.path.join (data_dir, 'logs') 
file_dir = os.path.join(data_dir, 'files')
upload_dir = os.path.join(file_dir, 'uploads')
log_file = os.path.join(log_dir, 'census.log')
