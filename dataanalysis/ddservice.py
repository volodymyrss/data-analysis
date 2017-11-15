import importlib
import json
import os
import threading

from flask import Flask
from flask_restful import Resource, Api, reqparse

from dataanalysis.printhook import log

dd_modules=[]

from dataanalysis.caches.resources import Response, jsonify

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
        return {'ping': 'pong','pid':os.getpid(),'thread':threading.current_thread().ident }

class List(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('modules', type=str, help='')
        parser.add_argument('assume', type=str, help='')
        args = parser.parse_args()

        import dataanalysis.core as da
        da.reset()
        import_ddmodules(args['modules'].split(","))

        return da.AnalysisFactory.cache.keys()

def interpret_simple_assume(assume_strings):
    r={}
    for assume_string in assume_strings.split(";"):
        setting,value=assume_string.split("=")
        objname,attr=setting.split(".")

        if objname not in r:
            r[objname]={}

        try:
            r[objname][attr]=json.loads(value)
        except Exception as e:
            raise Exception("unable to decode: "+value)

    return r.items()


class Produce(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('target', type=str, help='', required=True)
        parser.add_argument('modules', type=str, help='', required=True)
        parser.add_argument('assumptions', type=str, help='',default="[]")
        parser.add_argument('assume', type=str, help='', default="")
        parser.add_argument('expected_hashe', type=str, help='')
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

        assumptions = json.loads(args.get('assumptions'))
        print("ddservice got assumptions:")

        A=da.AnalysisFactory.byname(args.get('target'))

        assumptions+=interpret_simple_assume(args['assume'])

        for assumption in assumptions:
            a=da.AnalysisFactory.byname(assumption[0])
            a.import_data(assumption[1])
            print(a,"from",assumption)

        expected_hashe_str=args['expected_hashe']

        if expected_hashe_str is None:
            expected_hashe=None
        else:
            try:
                expected_hashe=json.loads(expected_hashe_str)
            except Exception as e:
                log("failed to interpret expected hashe:",expected_hashe_str)
                return Response(
                    status='error decoding expected hashe',
                    data=dict(
                        expected_hashe_str=expected_hashe_str,
                        decoding_exception=repr(e),
                    )
                ).jsonify()


        requested_by=["service",]+args['requested_by'].split(",")

        hashe,obj=A.process(output_required=False)

        if expected_hashe_str is not None and expected_hashe != jsonify(hashe):
            return Response(
                status='error',
                data=dict(comment="mismatch between expected hashe and producable",
                          expected_hashe=args['expected_hashe'],
                          producable_hashe=hashe,
                          ),
            ).jsonify()

        if args['mode'] == "fetch":
            log("No interactive produce requested for", A)
            A.produce_disabled = True
        elif args['mode'] == "interactive":
            log("interactive produce requested for",A)
        elif args['mode'] == "delayed":
            pass
        else:
            return Response(
                status='error',
                data=dict(comment="unknown produce mode"),
            ).jsonify()
        try:
            hashe, obj=A.process(output_required=True,requested_by=requested_by)

            if expected_hashe_str is not None and expected_hashe != jsonify(hashe):
                return Response(
                    status='error',
                    data=dict(comment="mismatch between expected hashe and produced",
                              expected_hashe=args['expected_hashe'],
                              producable_hashe=hashe,
                              ),
                ).jsonify()

            log("server succeeded to get the object", A)
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

        except da.AnalysisDelegatedException as e:
            log("service is waiting for dependencies:",e)
            return Response(
                status="waiting for dependencies",
                data=dict(
                    resources=A.guess_main_resources(),
                    dependencies=[r.jsonify() for r in e.resources],
                ),
            ).jsonify()


def create_app():
    app = Flask(__name__)
    api = Api(app)
    api_version="v0"
    api.add_resource(List, '/api/%s/list'%api_version)
    api.add_resource(Produce, '/api/%s/produce'%api_version)
    api.add_resource(Status, '/api/%s/status'%api_version)
    return app

if __name__ == '__main__':
    import dataanalysis as da
    da.debug_output()
    create_app().run(debug=True,port=6767)

