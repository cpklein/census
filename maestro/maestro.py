import subprocess, os, psutil
import uuid, yaml, json
from flask import Flask, jsonify, request, send_from_directory
import duckdb
from io import StringIO
import paramiko
import requests
import re
from requests.auth import HTTPBasicAuth
import logging
from logging.config import dictConfig
from zipfile import ZipFile
from census_local import *



# Create path if new
def check_path_and_create(path):
    if not os.path.exists(path):
        os.makedirs(path)

# File Configuration on package
meltano_dir = os.path.join(census_dir, 'meltano')
meltano_log_config_file = os.path.join(meltano_dir, 'logging_template.yaml')

# File Configuration data
db_dir = os.path.join(data_dir, 'database')
log_dir = os.path.join (data_dir, 'logs') 
file_dir = os.path.join(data_dir, 'files')
upload_dir = os.path.join(file_dir, 'uploads')
log_file = os.path.join(log_dir, 'census.log')

for path in [db_dir,
             log_dir,
             file_dir,
             upload_dir]:
    check_path_and_create(path)

# Logging Configuration
dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] — " + census_id + " — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
        }
    },
    "handlers": {
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": log_file,
            "maxBytes": 100000,
            "backupCount": 10,
            "delay": "False",
       },
        "syslog": {
            "class": "logging.handlers.SysLogHandler",
            "formatter": "default",
            "address": (syslog_server, 514),
            "facility": "user"
        }
    },
    "loggers": {
        "gunicorn.error": {
            "handlers": ["file", "syslog"] ,
            "level": "INFO",
            "propagate": False,
        },
        "gunicorn.access": {
            "handlers": ["file", "syslog"] ,
            "level": "INFO",
            "propagate": False,
        }
    },
    "root": {
        "level": "WARN",
        "handlers": ["file", "syslog"]
    }
})

    

# Global Variables

# List of opened connections
# each connection has tuple with "database" (string with db filename)
# "duck_conn" (duckdb connection object)
# "result" (Relation Object from previous execution for future fetch)
duck = []

def get_duck_conn(database):
    global duck
    for db_name, duck_conn, result in duck:
        if db_name == database:
            # We have found, return the connection
            app.logger.debug("Retrieved database connection: " + database)
            return duck_conn, result
    # If we haven't found, open a connection
    try:
        duck_conn = duckdb.connect(database=os.path.join(db_dir, database), read_only=False)
        # Add connection to online cache
        duck.append((database, duck_conn, None))
        app.logger.debug("Opened database connection: " + database)
    except Exception as error:
        app.logger.warning("Error opening database: " + error.args[0])
        duck_conn = None
    # return a recently created connection
    return duck_conn, None    

def close_duck_conn(database):
    global duck
    for db_name, duck_conn, result in duck:
        if db_name == database:
            # Remove from list
            duck.remove((database, duck_conn, result))
            # And close
            duck_conn.close()
            app.logger.debug("Closed database connection: " + database)
            return
    # We haven't found the connection
    app.logger.warning("Error closing database, database not found: " + database )
    return

def update_duck_conn(database, new_result):
    global duck
    for db_name, duck_conn, result in duck:
        if db_name == database:
            # Remove onld tuple
            duck.remove((database, duck_conn, result))
            # Add the update
            duck.append((database, duck_conn, new_result))
    app.logger.debug("Updated database connection: " + database)

app = Flask(__name__)

@app.route("/")
def hello():
    return "<h3>Census Test Page</h3>"

@app.route('/uploads/<name>')
def download_file(name):
    try:
        app.logger.debug("requested:" + name)
        return send_from_directory(upload_dir, name)
    except Exception as error:
        app.logger.debug("upload ERROR:" + error.args[1]) 
    
        


@app.route('/transfer/http', methods = ['POST'])
def get_http_file():
        
    body = request.get_json()
    # If extract == true, unzip the file at the end
    extract = request.args.get('extract', 'false')
    if extract == 'true':
        unzip = True
    else:
        unzip = False
    # Headers
    headers = {}
    # Query Parameters
    params= {}
    # Replace parameters inside the 'oauth2' information. This function is called by re.sub
    def repf(match):
        return body['account']['oauth2'][match.group(1)]
    def insert_token():
        # Insert the token in the proper position
        if body['account']['oauth2']['token_position'] == 'header':
            for param in body['account']['oauth2']['header']:
                # Replace if needed
                headers[param['key']] = re.sub('<>(.+)</>', repf, param['value'])
        
        
    # Build URL
    if body.get("rest", {}).get("full_url", None):
        url = body['rest']['full_url']
    else:            
        protocol = body['account']['protocol']
        host = body['account']['host'] 
        port = ':' + str(body['account']['port']) 
        path = body['rest']['path']     
        url = protocol + '://' + host + path

        # Build parameters
        for parameter in body['rest']['query']:
            params[parameter['key']] = parameter['value']
        #Build headers
        for parameter in body['rest']['header']:
            headers[parameter['key']] = parameter['value']
        
    #Data
    body_json =  body['rest']['body'] 
    
    #Authentication
    if body['account']['auth'] == 'basic':        
        auth = HTTPBasicAuth(body['account']['username'],
                             body['account']['password'])
    elif body['account']['auth'] == 'oauth2':
        auth = None
        insert_token()
    else:
        auth = None
    #File
    filename = os.path.join(file_dir, body['file']['local_path'], body['file']['filename'])
    
    #Chunk Size
    chunk_size = body['file']['chunck_size']
    
    # Response
    resp = {}
    try:        
        # read the file
        stream = requests.get(url, params=params, headers=headers, data=body_json, auth=auth, stream=True)
        # authentication failure and OAuth2
        if stream.status_code == 401 and body['account']['auth'] == "oauth2":
            app.logger.debug("OAuth Authentication Failure")
            # refresh token
            body['account']['oauth2']['access_token'] = refresh_access_token(body['account']['oauth2'])
            app.logger.debug("OAuth Token Refreshed")
            resp['new_token'] = body['account']['oauth2']['access_token']
            insert_token()
            # Try again 
            stream = requests.get(url, params=params, headers=headers, data=body_json, auth=auth, stream=True)
            app.logger.debug("Retry after OAuth Token Refreshed")
                        
        # Process only with 200 code
        if stream.status_code == 200:
            with open(filename, 'wb') as fd:
                for chunk in stream.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
            resp["status"] = "transfered"
            app.logger.debug("HTTP File Transfered Succeeded - Filename:" + filename)
            if unzip:
                with ZipFile(filename) as myzip:
                    myzip.extractall(path=os.path.join(file_dir,
                                                        body['file']['local_path']))
                app.logger.debug("File unzipped")
        else:
            resp["error"] = stream.text
            app.logger.warning("Error on HTTP File Request: " + stream.text)
    except Exception as error:
        resp["error"] = error.args[1]
        app.logger.warning("Error :" + error.args[1])
    
    return jsonify(resp)
            

def refresh_access_token(oauth2):
    params = {}
    # Replace parameters inside the 'oauth2' information. This function is called by re.sub
    def repf(match):
        return oauth2[match.group(1)]
    # Build the parameters of the refresh token request
    for param in oauth2['refresh_token_body_params']:
        params[param['key']] = re.sub('<>(.+)</>', repf, param['value'])
        
    resp = requests.post(
        oauth2['refresh_token_endpoint'], 
        params=params
        )
    resp_data = resp.json()
    return resp_data['access_token']


# Expected Body
#{
#    "tap" : "tap-s3-csv",
#    "target" : "target-duckdb"
#}
@app.route('/extractor',methods = ['POST'])
def go_process():
    body = request.get_json()
    tap = body.get('tap')
    target = body.get('target')
    #Get the output logfile
    exec_id = gen_log_config()
    process = subprocess.Popen(['meltano',
                                '--log-config=' + os.path.join(log_dir, exec_id + '.yaml'),
                                'run', 
                                tap, 
                                target], 
                               #stdout=subprocess.PIPE,
                               #stderr=subprocess.PIPE,
                               universal_newlines=True,
                               #cwd=r'/Users/caio/Development/integra/data/dataops01')
                               cwd=meltano_dir)

    ended = process.poll()
    if not ended:
        msg = "running"
    else:
        
        msg = "EXIT: " + str(ended)
    return jsonify({"status":msg,
                    "exec_id" : exec_id,
                    "pid" : process.pid})


@app.route('/extractor/<exec_id>',methods = ['GET'])
def get_extractor_log(exec_id):
    resp = {}
    with open(os.path.join(log_dir, exec_id + '.log')) as f:
        lines = f.readlines()
        json_log = []
        for line in lines:
            json_log.append(json.loads(line))
    resp['log'] = json_log
    pid = request.args.get('pid', None)
    if pid: 
        if psutil.pid_exists(int(pid)):
            process = psutil.Process(int(pid))
            status = process.status()
        else:
            status = 'not found'
        resp['status'] = status
    
    return jsonify(resp)

@app.route('/sql/execute/<database>', methods = ['POST'])
def sql_execute(database):
    body = request.get_json()
    query = body.get('query')
    duck_conn, result = get_duck_conn(database)
    if duck_conn != None:
        # We have a database
        try:
            result =  duck_conn.sql(query)
        except Exception as error:
            resp = {"error" : error.args[0]}
            app.logger.warning("error executed sql: " + query + " error: " + error.args[0])                
            close_duck_conn(database)
            return jsonify(resp)

        # Deal with DuckDB Relation
        if result != None:
            fetchmany = request.args.get('fetchmany', None)
            if fetchmany:
                resp = result.fetchmany(int(fetchmany))
                app.logger.debug("executed sql fetch: " + query + " fetchmany: " + fetchmany)
                if len(resp) < int(fetchmany):
                    # Close connection if we have read it all
                    close_duck_conn(database)
                else:
                    # Update connection with new result for future fetch
                    update_duck_conn(database, result)
            else:
                resp = result.fetchall()
                app.logger.debug("executed sql all: " + query)    
                close_duck_conn(database)
            # Return as a list of records - JSON
            if request.args.get('format', False) == 'list_of_records':
                resp = json_response(result, resp)
        # No response to deal with
        else:
            resp = { "status" : "executed"}
            app.logger.debug("executed sql: " + query)
            # Not fetchmany, so close the connection
            close_duck_conn(database)
    # We don't have a database
    else:
        resp = {"error" : "Couldn't open database: " + database}
        app.logger.warning("Couldn't open database: " + database)    
    return jsonify(resp)

@app.route('/sql/fetch/<database>', methods = ['POST'])
def fetch(database):
    duck_conn, result = get_duck_conn(database)
    fetchmany = request.args.get('fetchmany', None)
    if result != None :       
        try:
            if fetchmany:
                resp = result.fetchmany(int(fetchmany))
                app.logger.debug("executed fetchmany: " + fetchmany)
                # Close connection if we reached the end of Relation Object
                if len(resp) < int(fetchmany):
                    close_duck_conn(database)
                else:
                    update_duck_conn(database, result)
            else:
                resp = result.fetchall()
                app.logger.debug("executed fetchall ")
                close_duck_conn(database)
            if request.args.get('format', False) == 'list_of_records':
                resp = json_response(result, resp)
                
        except Exception as error:
            resp = {"error" : error.args[1]}
    else:
        resp = {"error" : "Couldn't find result for fetch"}
        app.logger.warning("Couldn't find result for fetch")    
        close_duck_conn(database)
    return jsonify(resp)

def json_response(result, resp):
    columns = result.columns
    new_resp =  [dict(zip(columns,register)) for register in resp]
    return new_resp
    

# Saves the json_data into the filename
# filename = Name of the file to store the json data
# json_data = Array of records to be imported into duckdb
@app.route('/transfer/json', methods = ['POST'])
def receive_json():
    body = request.get_json()
    filename = body.get('filename')
    local_path = body.get('local_path', '')
    json_data = body.get('json_data')
    data_str = json.dumps(json_data)
    try:
        with open(os.path.join(file_dir, local_path, filename), 'w') as f_out:
            f_out.write(data_str)
        resp = {"filename" : filename}
        app.logger.debug("saved json:" + filename + " bytes:" + str(len(data_str)))
    except Exception as error:
        resp = {"error" : error.args[1]}
        app.logger.warning("error saving json:" + filename + " error:" + error.args[1])
    return jsonify(resp)

#List files directory - Only files
@app.route('/files/list', methods = ['GET'])
def list_files():
    subdir = request.args.get('subdir', '')
    try:
        resp = [f for f in os.listdir(os.path.join(file_dir, subdir)) if os.path.isfile(os.path.join(os.path.join(file_dir, subdir), f))]
    except Exception as error:
        resp = {"error" : error.args[1]}
    
    app.logger.debug("directory:" + os.path.join(file_dir, subdir))
        
    return jsonify(resp)

#List files directory - Only files
@app.route('/logger_level', methods = ['POST'])
def set_logger_level():
    app.logger.setLevel(logging.DEBUG)
    level = request.args.get('level', '')
    if level == 'DEBUG':
        app.logger.setLevel(logging.DEBUG)
        new_level = 'DEBUG'
    else :
        app.logger.setLevel(logging.WARNING)
        new_level = 'WARNING'
    resp = {"level" : new_level}
    return jsonify(resp)


@app.route('/transfer/ssh/list', methods = ['GET'])
def list_remote_directory():
    body = request.get_json()
    private_key = body.get('private_key')
    username = body.get('username')
    server = body.get('server')
    port = body.get('port')
    directory = body.get('directory')
    #create a virtual file
    not_really_a_file = StringIO(private_key)
    # import as paramiko key
    p_key = paramiko.RSAKey.from_private_key(not_really_a_file)
    transport = paramiko.Transport((server,int(port)))
    transport.connect(username=username, pkey=p_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    # Get the list
    resp = sftp.listdir(path=directory)
    
    return jsonify(resp)
    
@app.route('/transfer/ssh/get', methods = ['POST'])
def get_remote_ssh():
    body = request.get_json()
    private_key = body.get('private_key')
    username = body.get('username')
    server = body.get('server')
    port = body.get('port')
    remote_path = body.get('remote_path')
    search_pattern = body.get('search_pattern')
    local_path = body.get('local_path')
    #create a virtual file
    not_really_a_file = StringIO(private_key)
    # import as paramiko key
    p_key = paramiko.RSAKey.from_private_key(not_really_a_file)
    
    #Get the list of files to be moves
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server, username=username, pkey=p_key)  
    rawcommand = 'find {path} -maxdepth 1 -name "{pattern}"'
    command = rawcommand.format(path=remote_path, pattern=search_pattern)
    stdin, stdout, stderr = ssh.exec_command(command)
    filelist = stdout.read().splitlines()
    ssh.close()
    
    
    #Move the files
    transport = paramiko.Transport((server,int(port)))
    transport.connect(username=username, pkey=p_key)
    files_moved = []
    try:
        sftp = paramiko.SFTPClient.from_transport(transport)
        #Iterate over the file list
        for afile in filelist:
            (head, filename) = os.path.split(afile)
            files_moved.append(filename.decode())
            
            # Move File
            sftp.get(afile, os.path.join(local_path, filename.decode()))
        resp = {"status" : "success", "files" : files_moved}
    except Exception as error:
        resp = {"status":"error","error" : error.args[1]}

    return jsonify(resp)

def gen_log_config():
    with open(meltano_log_config_file) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    exec_id = str(uuid.uuid4())
    #update config with specific log file for this execution 
    conf['handlers']['file']['filename'] = os.path.join(log_dir, exec_id + '.log')
    with open(os.path.join(log_dir, exec_id + '.yaml'), 'w') as f_out:
        yaml.dump(conf, f_out)
    return exec_id


if __name__ == "__main__":
    import os
    if 'WINGDB_ACTIVE' in os.environ:
        app.debug = False
    # Set DEBUG as default
    app.logger.setLevel(logging.DEBUG)
    app.run(host="0.0.0.0", port=8000)
