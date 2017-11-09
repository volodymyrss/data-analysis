#class DDEndpoint():
#    def __init__(self,target):
#        pass

# should list available entrypoints
# compelx insid


#class DDAFactoryService():
#    def __init__(self):
#        pass

from flask import Flask
from flask_restful import Resource, Api



#class DDAFactoryService(Resource):

class Ping(Resource):
    def get(self):
        return {'ping': 'pong'}

class List(Resource):
    def get(self):
        return {'hello1': 'world'}

class Run(Resource):
    def get(self):
        return {'hello2': 'world'}

def create_app():
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(List, '/list')
    api.add_resource(Run, '/run')
    api.add_resource(Ping, '/ping')
    return app

if __name__ == '__main__':
    create_app().run(debug=True)

