import subprocess, os, psutil
import uuid, yaml, json
from flask import Flask, jsonify, request
import duckdb
from io import StringIO
import paramiko
import requests
import re
from requests.auth import HTTPBasicAuth
import logging
from logging.config import dictConfig
from census_local import *

# Log Extra
#d = {'censusid': 'skyone_oci'}
#logging.config.fileConfig('logging.conf')
#logger = logging.getLogger('census_logger')
#logger.setLevel(logging.DEBUG)


dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] — " + censusid + " — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
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


# File Configuration
config_file = 'logging_template.yaml'
log_directory = '../logs'
config_directory = '../meltano'
run_directory = '../meltano'
file_directory = '../files'



duck_conn = None
result = None

app = Flask(__name__)

@app.route("/")
def hello():
    return "<h3>Census Test Page</h3>"

@app.route('/transfer/http', methods = ['POST'])
def get_http_file():
        
    body = request.get_json()
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
    filename = os.path.join(body['file']['local_path'], body['file']['filename'])
    
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
            app.logger.debug("HTTP File Transfered Succeeded")
        else:
            resp["error"] = stream.text
            app.logger.warning("Error on HTTP File Request: " + stream.text)
    except Exception as error:
        resp["error"] = error.args
        app.logger.warning("Error " + error.args)
    
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
                                '--log-config=' + os.path.join(log_directory, exec_id + '.yaml'),
                                'run', 
                                tap, 
                                target], 
                               #stdout=subprocess.PIPE,
                               #stderr=subprocess.PIPE,
                               universal_newlines=True,
                               #cwd=r'/Users/caio/Development/integra/data/dataops01')
                               cwd=run_directory)

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
    with open(os.path.join(log_directory, exec_id + '.log')) as f:
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
    global duck_conn
    global result
    try:
        if duck_conn == None:
            duck_conn = duckdb.connect(database=os.path.join(db_directory, database), read_only=False)
        fetchmany = request.args.get('fetchmany', None)
        if fetchmany:
            result = duck_conn.sql(query)
            resp = result.fetchmany(int(fetchmany))
        else:
            result = duck_conn.sql(query)
            if result != None:
                resp = result.fetchall()
            else:
                resp = { "status" : "executed"}
        if request.args.get('format', False) == 'list_of_records':
            resp = json_response(result, resp)
    except Exception as error:
        resp = {"error" : error.args}
    
    return jsonify(resp)

@app.route('/sql/fetch/<database>', methods = ['POST'])
def fetch(database):
    global duck_conn
    global result
    try:
        if duck_conn == None:
            duck_conn = duckdb.connect(database=os.path.join(db_directory, database), read_only=False)
        fetchmany = request.args.get('fetchmany', None)
        if fetchmany:
            resp = result.fetchmany(int(fetchmany))
        else:
            resp = result.fetchall()
        if request.args.get('format', False) == 'list_of_records':
            resp = json_response(result, resp)
            
    except Exception as error:
        resp = {"error" : error.args}
    
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
    json_data = body.get('json_data')
    data_str = json.dumps(json_data)
    try:
        with open(os.path.join(file_directory, filename), 'w') as f_out:
            f_out.write(data_str)
        resp = {"filename" : filename}
        app.logger.debug("saved json:" + filename + " bytes:" + str(len(data_str)))
    except Exception as error:
        resp = {"error" : error.args}
        app.logger.warning("error saving json:" + filename + " error:" + error.args)
    return jsonify(resp)

#List files directory - Only files
@app.route('/files/list', methods = ['GET'])
def list_files():
    subdir = request.args.get('subdir', '')
    try:
        resp = [f for f in os.listdir(os.path.join(file_directory, subdir)) if os.path.isfile(os.path.join(os.path.join(file_directory, subdir), f))]
    except Exception as error:
        resp = {"error" : error.args}
    
    app.logger.debug("directory:" + os.path.join(file_directory, subdir))
        
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
        resp = {"status":"error","error" : error.args}

    return jsonify(resp)

def gen_log_config():
    with open(os.path.join(config_directory, config_file)) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    exec_id = str(uuid.uuid4())
    #update config with specific log file for this execution 
    conf['handlers']['file']['filename'] = os.path.join(log_directory, exec_id + '.log')
    with open(os.path.join(log_directory, exec_id + '.yaml'), 'w') as f_out:
        yaml.dump(conf, f_out)
    return exec_id


if __name__ == "__main__":
    import os
    if 'WINGDB_ACTIVE' in os.environ:
        app.debug = False
    # Set DEBUG as default
    app.logger.setLevel(logging.DEBUG)
    app.run(host="0.0.0.0", port=8000)