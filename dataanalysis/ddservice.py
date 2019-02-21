import json
import os
import threading

from flask import Flask
from flask_restful import Resource, Api, reqparse

import dataanalysis.core as da
from dataanalysis import emerge
from dataanalysis.caches.resources import Response, jsonify
from dataanalysis.printhook import log


# exception

class Status(Resource):
    def get(self):
        return {'ping': 'pong','pid':os.getpid(),'thread':threading.current_thread().ident }

class List(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('modules', type=str, help='')
        parser.add_argument('assume', type=str, help='')
        args = parser.parse_args()

        da.reset()
        emerge.import_ddmodules(args['modules'].split(","))

        return list(da.AnalysisFactory.cache.keys())

def interpret_simple_assume(assume_strings):
    r={}
    log("assume strings","\""+assume_strings+"\"")
    if assume_strings.strip()=="":
        return r

    for assume_string in assume_strings.split(";"):
        log("simple asssume:","\""+assume_string+"\"")
        if assume_string.strip()=="":
            continue
        setting,value=assume_string.split("=")
        log("simple asssume obj:",setting)
        objname,attr=setting.split(".")

        if objname not in r:
            r[objname]={}

        try:
            r[objname][attr]=json.loads(value)
        except Exception as e:
            raise Exception("unable to decode: "+value)

    return list(r.items())


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

        print(("ddservice request:",args['request_comment'],args['request_id']))
        print(("ddservice produce args",args))

        expected_hashe_str = args['expected_hashe']

        if expected_hashe_str is None:
            print("no expected hashe!")
            expected_hashe = None
        else:
            try:
                expected_hashe = json.loads(expected_hashe_str)
            except Exception as e:
                log("failed to interpret expected hashe:", expected_hashe_str)
                return Response(
                    status='error decoding expected hashe',
                    data=dict(
                        expected_hashe_str=expected_hashe_str,
                        decoding_exception=repr(e),
                    )
                ).jsonify()

        modules=args['modules'].split(",")

        assumptions=json.loads(args.get('assumptions'))

        log("injected assumptions",assumptions)

        assumptions += interpret_simple_assume(args['assume'])

        requested_by = ["service", ] + args['requested_by'].split(",")

        try:
            emerged_object=emerge.emerge_from_identity(da.DataAnalysisIdentity(
                factory_name=args['target'],
                full_name=None,
                modules=modules,
                assumptions=assumptions,
                expected_hashe=expected_hashe,
            ))
        except emerge.InconsitentEmergence as e:
            return Response(
                status='error',
                data=dict(comment="found mismatch between expected hashe and producable",
                          expected_hashe=args['expected_hashe'],
                          producable_hashe=e.cando,
                          ),
            ).jsonify()

        if args['mode'] == "fetch":
            log("No interactive produce requested for", emerged_object)
            emerged_object.produce_disabled = True
        elif args['mode'] == "interactive":
            log("interactive produce requested for",emerged_object)
        elif args['mode'] == "delayed":
            pass
        else:
            return Response(
                status='error',
                data=dict(comment="unknown produce mode"),
            ).jsonify()
        try:
            hashe, obj=emerged_object.process(output_required=True,requested_by=requested_by)

            if expected_hashe_str is not None and jsonify(expected_hashe) != jsonify(hashe):
                return Response(
                    status='error',
                    data=dict(comment="mismatch between expected hashe and produced", # TODO why repeat?
                              expected_hashe=args['expected_hashe'],
                              producable_hashe=hashe,
                              ),
                ).jsonify()

            log("server succeeded to get the object", emerged_object)
            return Response(
                status='result',
                data=emerged_object.export_data(include_class_attributes=True),
            ).jsonify()

        except da.ProduceDisabledException as e:
            log("no result while produce disabled for",emerged_object)
            fih, o = emerged_object.process(output_required=False)
            return Response(
                status="not allowed to produce",
                data=dict(resources=emerged_object.guess_main_resources()),
            ).jsonify()

        except da.AnalysisDelegatedException as e:
            log("service is waiting for dependencies:",e)
            return Response(
                status="waiting for dependencies",
                data=dict(
                    resources=emerged_object.guess_main_resources(),
                    dependencies=[r.jsonify() for r in e.resources],
                ),
            ).jsonify()

class Callback(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('target', type=str, help='')
        args = parser.parse_args()



def create_app():
    app = Flask(__name__)
    api = Api(app)
    api_version="v0"
    api.add_resource(List, '/api/%s/list'%api_version)
    api.add_resource(Produce, '/api/%s/produce'%api_version)
    api.add_resource(Status, '/api/%s/status'%api_version)
    api.add_resource(Callback, '/api/%s/callback' % api_version)

    return app

if __name__ == '__main__':
    import dataanalysis as da
    da.debug_output()
    create_app().run(debug=False,port=6767)

