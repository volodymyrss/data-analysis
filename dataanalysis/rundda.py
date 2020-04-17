#!/bin/env python

#TODO all this is very bad. fix

import argparse
import json
import sys
import urllib.request, urllib.parse, urllib.error
import shutil

import yaml as yaml

from dataanalysis.caches.queue import QueueCache
from dataanalysis.printhook import get_local_log

log=get_local_log(__name__)


def main():
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
    parser.add_argument('-S', dest='very_silent',  help='...',action='store_true', default=False)
    parser.add_argument('-v', dest='verbose',  help='...',action='store_true', default=False)
    parser.add_argument('-V', dest='very_verbose',  help='...',action='store_true', default=False)
    parser.add_argument('-x', dest='failsafe',  help='...',action='store_true', default=False)
    parser.add_argument('-c', dest='cachelink',  help='...',action='store_true', default=False)
    parser.add_argument('-f', dest='force_run', metavar='ANALYSISNAME', type=str, help='analysis to run', nargs='+', action='append', default=[])
    parser.add_argument('-F', dest='force_produce', metavar='ANALYSISNAME', type=str, help='analysis to run', nargs='+', action='append', default=[])
    parser.add_argument('-d', dest='disable_run', metavar='ANALYSISNAME', type=str, help='analysis to disable run', nargs='+', action='append', default=[])
    parser.add_argument('-Q', dest='delegate_to_queue', metavar='QUEUE', type=str, help='delegate to queue',default=None)
    parser.add_argument('-D', dest='prompt_delegate_to_queue', metavar='QUEUE', type=str, help='delegate to queue',default=None)
    parser.add_argument('-I', dest='isolate',  help='...',action='store_true', default=True)
    parser.add_argument('-Ic', dest='isolate_cleanup',  help='...',action='store_true', default=False)
    parser.add_argument('--delegate-target', dest='delegate_target', action="store_true",  help='delegate target',default=False)
    parser.add_argument('--callback', dest='callback', metavar='QUEUE', type=str, help='delegate to queue',default=None)

    args = parser.parse_args()

    log(args.module)

    from dataanalysis import core, importing
    import dataanalysis.printhook

    if args.verbose:
        log("will be chatty")
        dataanalysis.printhook.global_log_enabled=True
        dataanalysis.printhook.global_fancy_output=True
        dataanalysis.printhook.global_permissive_output=True
    else:
        dataanalysis.printhook.global_log_enabled=False
        dataanalysis.printhook.global_fancy_output=False

    if args.failsafe:
        log("will be chatty and safe")
        dataanalysis.printhook.global_log_enabled=True
        dataanalysis.printhook.global_fancy_output=False
        dataanalysis.printhook.global_permissive_output=True


    if args.quiet:
        log("will be quiet")
        dataanalysis.printhook.LogStream(None, lambda x: set(x) & set(['top', 'main']))
    else:
        log("will not be quiet")
        dataanalysis.printhook.LogStream(None, lambda x:True)

    if args.very_verbose:
        dataanalysis.printhook.global_permissive_output=True

    if args.very_silent:
        dataanalysis.printhook.global_suppress_output=True
        dataanalysis.printhook.global_permissive_output=False
        dataanalysis.printhook.global_log_enabled=False
        dataanalysis.printhook.global_fancy_output=False

    modules=[m[0] for m in args.module]

    injected_objects=[]
    for inj_fn, in args.inject:
        log("injecting from",inj_fn)
        injected_objects.append(json.load(open(inj_fn)))

    injected_objects.append((args.object_name,dict(request_root_node=True)))

    if args.prompt_delegate_to_queue:
        identity=dataanalysis.DataAnalysisIdentity(
            factory_name=args.object_name,
            full_name=args.object_name,
            modules=dataanalysis.AnalysisFactory.format_module_description(modules),
            assumptions=[('',a[0]) for a in args.assume]+[(a,b) for a,b in injected_objects],
            expected_hashe=None,
        )

        log("generated identity",identity)

        cache=QueueCache(args.prompt_delegate_to_queue.strip())

        log("cache:",cache,"from",args.prompt_delegate_to_queue)
        log("queue status before", cache.queue.info)

        delegation_state=cache.queue.put(
            dict(
                object_identity=identity.serialize(),

            ),
            submission_data=dict(
                request_origin="command_line",
                callbacks=[args.callback],
            )
        )

        log("queue status now",cache.queue.info)
        log("put in cache, exiting")

        e=dataanalysis.AnalysisDelegatedException(None)

        if delegation_state is not None and delegation_state['state']=="done":
            log("the prompt delegation already done, disabling run and hoping for results")
            args.disable_run.append([args.object_name])
        elif delegation_state is not None and delegation_state['state']=="failed":
            log("the prompt delegation already done and failed, raising exception")

            failure=delegation_state
            failed_task=failure['task_entry']
            log("failed task",failed_task)
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
            log("delegation exception", e)
            yaml.dump(
                dict(
                    exception_type="delegation",
                    exception=repr(e),
                    hashe=e.hashe,
                    resources=[],
                    source_exceptions=None,
                    comment=None,
                    origin=None,
                    delegation_state=delegation_state['state'] if delegation_state is not None else 'submitted',
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
        log("importing",m)

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
        log("assumptions:",assumptions)

        assumptions_evaluated=eval(assumptions)
        if type(assumptions_evaluated) not in (list,tuple):
            assumptions_evaluated=[assumptions_evaluated]

        for i,assumption in enumerate(assumptions_evaluated):
            log("assumption from commandline",assumption)
            core.AnalysisFactory.WhatIfCopy('commandline_%i'%i, assumption)


    A= core.AnalysisFactory[args.object_name]()

    dataanalysis.callback.Callback.set_callback_accepted_classes([A.__class__])


    log(A)

    for a in args.force_run:
        log("force run",a)
        try:
            b= core.AnalysisFactory[a[0]]()
            b.__class__.cached=False
        except: # oh now!
            pass

    for a in args.force_produce:
        log("force produce",a)
        try:
            b= core.AnalysisFactory[a[0]]()
            b.__class__.read_caches=[]
        except: # oh now!
            pass

    for a in args.disable_run:
        log("disable run",a)
        b= core.AnalysisFactory[a[0]]()
        b.__class__.produce_disabled=True

    for i,inj_content in enumerate(injected_objects):
        #core.AnalysisFactory.inject_serialization(inj_content)
        assumption_from_injection=core.AnalysisFactory.implement_serialization(inj_content)
        log("assumption from injection",i,assumption_from_injection)
        log("assumption from injection derived from", inj_content)
        core.AnalysisFactory.WhatIfCopy('commandline_injection_%i' % i, assumption_from_injection)

    if args.delegate_to_queue is not None:
        log("delegate to queue:",args.delegate_to_queue)
        qcache=QueueCache(args.delegate_to_queue)
        A.cache.tail_parent(qcache)
        A.read_caches.append(qcache.__class__)


    if args.isolate or args.isolate_cleanup:
        isolated_directory_key="command_line"
    else:
        isolated_directory_key=None

    try:
        A._da_delegation_allowed=args.delegate_target
        #A.process(output_required=True,requested_by=["command_line"],callback_url=args.callback)
        A.get(requested_by=["command_line"],callback_url=args.callback,isolated_directory_key=isolated_directory_key)
        A.raise_stored_exceptions()
    except dataanalysis.AnalysisException as e:
        yaml.dump(
                  dict(
                      exception_type="node",
                      exception=e,
                    ),
                  open("exception.yaml","w"),
                  default_flow_style=False,
            )
        raise
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
        log("delegation exception",e)
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
        log("graph exception",e)
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
        log("will dump serialization to json")
        json.dump(A.export_data(embed_datafiles=True,verify_jsonifiable=True),open("object_data.json","w"), sort_keys=True,
                          indent=4, separators=(',', ': '))

    if args.serialize_json:
        fn = A.get_factory_name() + "_data.json"
        json.dump(A.serialize(embed_datafiles=True, verify_jsonifiable=True), open(fn, "w"),
                  sort_keys=True,
                  indent=4, separators=(',', ': '))

    if args.tar:
        log("will tar cache")
        log(A._da_cached_path)

    if args.cachelink:
        if hasattr(A,'_da_cached_pathes'):
            log("will note cache link")
            open("object_url.txt","w").write("".join([args.object_name+" "+dcp+"\n" for dcp in A._da_cached_pathes]))


    if args.isolate_cleanup:
        log("isolate cleanup:",A._da_isolated_directory,level='top')
        shutil.rmtree(A._da_isolated_directory)

