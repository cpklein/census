#Local Environment variables
import socket

if socket.gethostname() in ['german_shepherd', '1125-CKLEIN.local']:
    censusid = "DEVELOPER" 
    db_directory = '../database'
    log_file = "census.log"
else:
    censusid = "PROTOTYPE 01" 
    log_file = "/home/ubuntu/census/logs/census.log"
    db_directory = '/data/census/database'
    
syslog_server = "ec2-3-239-79-152.compute-1.amazonaws.com"
