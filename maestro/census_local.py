#Local Environment variables
import socket

census_id = socket.gethostname() 

# development environment
if census_id in ['german_shepherd', '1125-CKLEIN.local']:
    census_dir = '/Users/caioklein/Development/census'
    data_dir = '/Users/caioklein/Development/data'
# production environment
else:
    census_dir = '/home/ubuntu/census'
    data_dir = '/data/census'

# syslog server    
syslog_server = "ec2-3-239-79-152.compute-1.amazonaws.com"
