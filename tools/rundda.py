#!/bin/env python

import argparse
import json
import sys

import yaml

from dataanalysis.caches.queue import QueueCache

parser = argparse.ArgumentParser(description='Run a DDA object')
parser.add_argument('object_name', metavar='OBJECT_NAME', type=str, help='name of the object')
parser.add_argument('-m', dest='module', metavar='MODULE_NAME', type=str, help='module to load', nargs='+', action='append', default=[])
parser.add_argument('-a', dest='assume', metavar='ASSUME', type=str, help='...', nargs='+', action='append', default=[])
parser.add_argument('-j', dest='json',  help='...',action='store_true', default=False)
parser.add_argument('-J', dest='serialize_json',  help='...',action='store_true', default=False)
parser.add_argument('-i', dest='inject', metavar='INJECT_JSON', type=str, help='json filename to inject', nargs='+', action='append', default=[])
parser.add_argument('-t', dest='tar',  help='...',action='store_true', default=False)
parser.add_argument('-q', dest='quiet',  help='...',action='store_true', default=False)
parser.add_argument('-s', dest='silent',  help='...',action='store_true', default=False)
parser.add_argument('-v', dest='verbose',  help='...',action='store_true', default=False)
parser.add_argument('-V', dest='very_verbose',  help='...',action='store_true', default=False)
parser.add_argument('-x', dest='failsafe',  help='...',action='store_true', default=False)
parser.add_argument('-c', dest='cachelink',  help='...',action='store_true', default=False)
parser.add_argument('-f', dest='force_run', metavar='ANALYSISNAME', type=str, help='analysis to run', nargs='+', action='append', default=[])
parser.add_argument('-F', dest='force_produce', metavar='ANALYSISNAME', type=str, help='analysis to run', nargs='+', action='append', default=[])
#parser.add_argument('-v', dest='verbose', metavar='ANALYSISNAME', type=str, help='analysis to verify only', nargs='+', action='append', default=[])
parser.add_argument('-d', dest='disable_run', metavar='ANALYSISNAME', type=str, help='analysis to disable run', nargs='+', action='append', default=[])
parser.add_argument('-Q', dest='delegate_to_queue', metavar='QUEUE', type=str, help='delegate to queue',default=None)
parser.add_argument('-D', dest='prompt_delegate_to_queue', metavar='QUEUE', type=str, help='delegate to queue',default=None)
parser.add_argument('--callback', dest='callback', metavar='QUEUE', type=str, help='delegate to queue',default=None)

args = parser.parse_args()

print(args.module)

from dataanalysis import core, importing
import dataanalysis.printhook

if args.verbose:
    print("will be chatty")
    dataanalysis.printhook.global_log_enabled=True
    dataanalysis.printhook.global_fancy_output=True
    dataanalysis.printhook.global_permissive_output=True
else:
    dataanalysis.printhook.global_log_enabled=False
    dataanalysis.printhook.global_fancy_output=False

if args.failsafe:
    print("will be chatty and safe")
    dataanalysis.printhook.global_log_enabled=True
    dataanalysis.printhook.global_fancy_output=False
    dataanalysis.printhook.global_permissive_output=True


if args.quiet:
    print("will be quiet")
    dataanalysis.printhook.LogStream(None, lambda x: set(x) & set(['top', 'main']))
else:
    print("will not be quiet")
    dataanalysis.printhook.LogStream(None, lambda x:True)

if args.very_verbose:
    dataanalysis.printhook.global_permissive_output=True

modules=[m[0] for m in args.module]

injected_objects=[]
for inj_fn, in args.inject:
    print("injecting from",inj_fn)
    injected_objects.append(json.load(open(inj_fn)))


if args.prompt_delegate_to_queue:
    identity=dataanalysis.DataAnalysisIdentity(
        factory_name=args.object_name,
        full_name=args.object_name,
        modules=dataanalysis.AnalysisFactory.format_module_description(modules),
        assumptions=[('',a[0]) for a in args.assume]+[(a,b) for a,b in injected_objects],
        expected_hashe=None,
    )

    print("generated identity",identity)

    cache=QueueCache(args.prompt_delegate_to_queue)

    print("cache:",cache,"from",args.prompt_delegate_to_queue)
    print("queue status before", cache.queue.info)

    delegation_state=cache.queue.put(
        dict(
            object_identity=identity.serialize(),

        ),
        submission_data=dict(
            request_origin="command_line",
            callbacks=[args.callback],
        )
    )

    print("queue status now",cache.queue.info)
    print("put in cache, exiting")

    e=dataanalysis.AnalysisDelegatedException(None)

    if delegation_state is not None and any([d['state']=="done" for d in delegation_state]):
        print("the prompt delegation already done, disabling run and hoping for results")
        args.disable_run.append([args.object_name])
    elif delegation_state is not None and any([d['state'] == "failed" for d in delegation_state]):
        print("the prompt delegation already done and failed, raising exception")

        failures=[d for d in delegation_state if d['state'] == "failed"]

        if len(failures)>1:
            yaml.dump(
                dict(
                    exception_type="queue",
                    exception=repr(e),
                    hashe=e.hashe,
                    resources=[],
                    source_exceptions=None,
                    comment=None,
                    origin=None,
                    delegation_state=delegation_state,
                ),
                open("exception.yaml", "w"),
                default_flow_style=False,
            )
        else:
            failure=failures[0]
            failed_task=yaml.load(open(failure['fn']))
            print("failed task",failed_task)
            failed_task_execution_info=failed_task['execution_info']

            yaml.dump(
                dict(
                    exception_type="analysis",
                    exception=failed_task_execution_info['exception'][0],
                    hashe=None,
                    resources=[],
                    source_exceptions=None,
                    comment=None,
                    origin=None,
                    delegation_state=failure['state'],
                ),
                open("exception.yaml", "w"),
                default_flow_style=False,
            )
            sys.exit(1)

        #args.disable_run.append([args.object_name])
    else:
        print("delegation exception", e)
        yaml.dump(
            dict(
                exception_type="delegation",
                exception=repr(e),
                hashe=e.hashe,
                resources=[],
                source_exceptions=None,
                comment=None,
                origin=None,
                delegation_state=delegation_state[0]['state'] if delegation_state is not None else 'submitted',
            ),
            open("exception.yaml", "w"),
            default_flow_style=False,
        )

        raise e

import requests

if args.callback and args.callback.startswith("http://"):
    params = dict(
        level='modules',
        node=args.object_name,
        message="loading modules",
        action="progress",
    )
    requests.get(args.callback,
                 params=params)

for m in modules:
    print "importing",m

    sys.path.append(".")
    module,name= importing.load_by_name(m)
    globals()[name]=module

if args.callback and args.callback.startswith("http://"):
    params = dict(
        level='modules',
        node=args.object_name,
        message="done loading modules",
        action="progress",
    )
    requests.get(args.callback,
                 params=params)



if len(args.assume)>0:
    assumptions = ",".join([a[0] for a in args.assume])
    print("assumptions:",assumptions)
    core.AnalysisFactory.WhatIfCopy('commandline', eval(assumptions))


A= core.AnalysisFactory[args.object_name]()

dataanalysis.callback.Callback.set_callback_accepted_classes([A.__class__])


print(A)

for a in args.force_run:
    print "force run",a
    try:
        b= core.AnalysisFactory[a[0]]()
        b.__class__.cached=False
    except: # oh now!
        pass

for a in args.force_produce:
    print("force produce",a)
    try:
        b= core.AnalysisFactory[a[0]]()
        b.__class__.read_caches=[]
    except: # oh now!
        pass

for a in args.disable_run:
    print("disable run",a)
    b= core.AnalysisFactory[a[0]]()
    b.__class__.produce_disabled=True

for inj_content, in injected_objects:
    core.AnalysisFactory.inject_serialization(inj_content)

if args.delegate_to_queue is not None:
    from dataanalysis.caches.queue import QueueCache
    qcache=QueueCache(args.delegate_to_queue)
    A.cache.tail_parent(qcache)
    A.read_caches.append(qcache.__class__)

try:
    A.process(output_required=True,requested_by=["command_line"],callback_url=args.callback)
except dataanalysis.UnhandledAnalysisException as e:
    yaml.dump(
              dict(
                  exception_type="node",
                  analysis_node_name=e.argdict['requested_by'],
                  requested_by=e.argdict['requested_by'],
                  main_log=e.argdict['main_log'],
                  traceback=e.argdict['tb'],
                ),
              open("exception.yaml","w"),
              default_flow_style=False,
        )
    raise
except dataanalysis.AnalysisDelegatedException as e:
    print("delegation exception",e)
    yaml.dump(
        dict(
            exception_type="delegation",
            exception=repr(e),
            hashe=e.hashe,
            resources=e.resources,
            source_exceptions=e.source_exceptions,
            comment=e.comment ,
            origin=e.origin,
        ),
        open("exception.yaml", "w"),
        default_flow_style=False,
    )
    raise
except Exception as e:
    print("graph exception",e)
    yaml.dump(
        dict(
            exception_type="graph",
            exception=repr(e),
        ),
        open("exception.yaml", "w"),
        default_flow_style=False,
    )
    raise


if args.json:
    print "will dump serialization to json"
    json.dump(A.export_data(embed_datafiles=True,verify_jsonifiable=True),open("object_data.json","w"), sort_keys=True,
                      indent=4, separators=(',', ': '))

if args.serialize_json:
    fn = A.get_factory_name() + "_data.json"
    json.dump(A.serialize(embed_datafiles=True, verify_jsonifiable=True), open(fn, "w"),
              sort_keys=True,
              indent=4, separators=(',', ': '))

if args.tar:
    print "will tar cache"
    print A._da_cached_path

if args.cachelink:
    if hasattr(A,'_da_cached_pathes'):
        print "will note cache link"
        open("object_url.txt","w").write("".join([args.object_name+" "+dcp+"\n" for dcp in A._da_cached_pathes]))




