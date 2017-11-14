import importlib

from flask import Flask
from flask_restful import Resource, Api, reqparse

from dataanalysis.printhook import log

dd_modules=[]

from dataanalysis.caches.resources import Response

# exception

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
        parser.add_argument('target', type=str, help='', required=True)
        parser.add_argument('modules', type=str, help='', required=True)
        parser.add_argument('assumptions', type=str, help='')
        parser.add_argument('mode', type=str, help='',default="interactive")
        parser.add_argument('requested_by', type=str, help='',default="")
        parser.add_argument('request_id', type=str, help='')
        parser.add_argument('request_comment', type=str, help='')
        args = parser.parse_args()

        print("ddservice request:",args['request_comment'],args['request_id'])
        print("ddservice produce args",args)

        import dataanalysis.core as da
        da.reset()
        import_ddmodules(args.get('modules').split(","))

        A=da.AnalysisFactory.byname(args.get('target'))

        requested_by=["service","->",]+args['requested_by'].split(",")

        if args['mode'] == "interactive":
            log("interactive produce requested for",A)
            A.get()
            return Response(
                status="result",
                data=A.export_data(include_class_attributes=True),
            ).jsonify()
        elif args['mode'] == "fetch":
            log("No interactive produce requested for", A)
            A.produce_disabled=True

            try:
                A.get(requested_by=requested_by)
                log("no produce extracted", A)
                return Response(
                    status='result',
                    data=A.export_data(include_class_attributes=True),
                ).jsonify()
            except da.ProduceDisabledException as e:
                log("no result while produce disabled for",A)
                fih, o = A.process(output_required=False)
                return Response(
                    status="not allowed to produce",
                    data=dict(resources=A.guess_main_resources()),
                ).jsonify()
        #elif args['mode'] == "delayed":
        #    pass
        else:
            raise Exception("unknown produce mode:"+args['mode'])


            #return fih



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

