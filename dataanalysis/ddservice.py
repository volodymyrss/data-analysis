import importlib

from flask import Flask
from flask_restful import Resource, Api

dd_modules=[]

def import_ddmodules():
    modules=[importlib.import_module(dd_module) for dd_module in dd_modules]
    for m in modules:
        reload(m)

    return modules

class Status(Resource):
    def get(self):
        return {'ping': 'pong'}

class List(Resource):
    def get(self):
        import dataanalysis.core as da
        da.reset()
        import_ddmodules()

        return da.AnalysisFactory.cache.keys()

class Produce(Resource):
    def get(self,target):
        import dataanalysis.core as da
        da.reset()
        import_ddmodules()

        A=da.AnalysisFactory.byname(target)
        A.get()

        return A.export_data(include_class_attributes=True)

def create_app():
    dd_modules.append('ddmoduletest')
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(List, '/list')
    api.add_resource(Produce, '/produce/<string:target>')
    api.add_resource(Status, '/status')
    return app

if __name__ == '__main__':
    create_app().run(debug=True)

