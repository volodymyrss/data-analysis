import importlib

from flask import Flask
from flask_restful import Resource, Api, reqparse

dd_modules=[]

def import_ddmodules(modules=None):
    if modules is None:
        modules=dd_modules

    modules=[importlib.import_module(dd_module) for dd_module in modules]
    for m in modules:
        reload(m)

    return modules

class Status(Resource):
    def get(self):
        return {'ping': 'pong'}

class List(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('modules', type=str, help='')
        parser.add_argument('assumptions', type=str, help='')
        args = parser.parse_args()

        import dataanalysis.core as da
        da.reset()
        import_ddmodules(args['modules'].split(","))

        return da.AnalysisFactory.cache.keys()

class Produce(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('target', type=str, help='')
        parser.add_argument('modules', type=str, help='')
        parser.add_argument('assumptions', type=str, help='')
        args = parser.parse_args()

        print("args",args)

        import dataanalysis.core as da
        da.reset()
        import_ddmodules(args['modules'].split(","))

        A=da.AnalysisFactory.byname(args['target'])
        A.get()

        return A.export_data(include_class_attributes=True)

def create_app():
    app = Flask(__name__)
    api = Api(app)
    api_version="v0"
    api.add_resource(List, '/api/%s/list'%api_version)
    api.add_resource(Produce, '/api/%s/produce'%api_version)
    api.add_resource(Status, '/api/%s/status'%api_version)
    return app

if __name__ == '__main__':
    create_app().run(debug=True,port=6767)

