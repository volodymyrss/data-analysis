#!flask/bin/python
import sys
from flask import Flask, url_for, jsonify, send_file, request
import pylru
import requests
import json

import io
import numpy as np

app = Flask(__name__)

import tempfile
import os
import time
import socket

def run_da(object,modules,assumptions):
    modules=" ".join([" -m "+m for m in modules.split(",")])
    assumptions=(" -a '%s'"%assumptions) if assumptions!="" else ""

    td=tempfile.mkdtemp()
    command="export UENV_ACTIVE=\"\"; source /home/savchenk/.uenv/bin/uenv.sh ddosa; cd %s; pwd; ls -lotr;"%td
    #runcommand="rundda.py %s -j %s %s "%(object,modules,assumptions)
    runcommand="rundda.py %s -j %s %s > /home/savchenk/spool/logs/jobs/%s_%i_${HOSTNAME}_`date +%%s` 2>&1"%(object,modules,assumptions,object,app.port)
    print("command:",command)
    print("run command:",runcommand)

    
    open("/home/savchenk/spool/logs/log_%i"%app.port,"a").write(str(time.time())+" "+socket.gethostname()+" "+runcommand+"\n")

    os.system(command+runcommand)


    result=json.load(open(td+"/object_data.json"))
    os.system("rm -rfv "+td)
    return result


@app.route('/analysis/api/v1.0/<string:object>', methods=['GET'])
def get_analysis(object):
    print(request.args)

    modules=request.args['modules'] if 'modules' in request.args else ""
    assumptions=request.args['assume'] if 'assume' in request.args else ""

    print("modules",modules)
    print("assumptions",assumptions)

    return jsonify(run_da(object,modules,assumptions))

if __name__ == '__main__':
    app.port=int(sys.argv[1])
    app.run(debug=True,port=app.port)

