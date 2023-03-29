import subprocess, os, psutil
import uuid, yaml, json
from flask import Flask, jsonify, request
import duckdb



config_file = 'logging_template.yaml'
log_directory = '../logs'
config_directory = '../meltano'
run_directory = '../meltano'
db_directory = '../database'
file_directory = '../files'

duck_conn = None
result = None

app = Flask(__name__)

@app.route("/")
def hello():
    return "<h3>Census Test Page</h3>"

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
            resp = duck_conn.sql(query).fetchall()
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
    try:
        with open(os.path.join(file_directory, filename), 'w') as f_out:
            f_out.write(json.dumps(json_data))
        resp = {"filename" : filename}
    except Exception as error:
        resp = {"error" : error.args}
    return jsonify(resp)

#List files directory - Only files
@app.route('/files/list', methods = ['GET'])
def list_files():
    subdir = request.args.get('subdir', '')
    onlyfiles = [f for f in os.listdir(os.path.join(file_directory, subdir)) if os.path.isfile(os.path.join(os.path.join(file_directory, subdir), f))]
    return jsonify(onlyfiles)
    
    
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
    app.run(host="0.0.0.0", port=8000)
