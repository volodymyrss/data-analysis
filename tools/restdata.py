#!flask/bin/python
from flask import Flask, url_for, jsonify, send_file, request
import pylru
import requests
import json

import StringIO
import numpy as np

app = Flask(__name__)

import tempfile
import os

def run_da(object,modules,assumptions):
    ir="/Integral/throng/savchenk/projects/spiacs/"

    modules=" ".join([" -m "+m for m in modules.split(",")])
    assumptions=(" -a '%s'"%assumptions) if assumptions!="" else ""

    td=tempfile.mkdtemp()
    command="export UENV_ACTIVE=\"\"; source /home/savchenk/.uenv/bin/uenv.sh ddosa; cd %s; pwd; ls -lotr;"%td
    runcommand="rundda.py %s -j %s %s"%(object,modules,assumptions)
    print "command:",command
    print "run command:",runcommand
    os.system(command+runcommand)

    return json.load(open("object_dump.json"))


@app.route('/analysis/api/v1.0/<string:object>', methods=['GET'])
def get_analysis(object):
    print request.args

    modules=request.args['modules'] if 'modules' in request.args else ""
    assumptions=request.args['assume'] if 'assume' in request.args else ""

    print "modules",modules
    print "assumptions",assumptions

    return jsonify(run_da(object,modules,assumptions))

if __name__ == '__main__':
    app.run(debug=True)

