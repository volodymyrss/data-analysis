#!/bin/env python

import argparse
import importing

parser = argparse.ArgumentParser(description='Run a DDA object')
parser.add_argument('object_name', metavar='OBJECT_NAME', type=str, help='name of the object')
parser.add_argument('-m', dest='module', metavar='MODULE_NAME', type=str, help='module to load', nargs='+', action='append', default=[])
parser.add_argument('-a', dest='assume', metavar='ASSUME', type=str, help='...', nargs='+', action='append', default=[])
parser.add_argument('-p', dest='plotit',  help='...',action='store_true', default=False)
parser.add_argument('-q', dest='quiet',  help='...',action='store_true', default=False)
parser.add_argument('-s', dest='silent',  help='...',action='store_true', default=False)
parser.add_argument('-v', dest='verbose',  help='...',action='store_true', default=False)
parser.add_argument('-f', dest='force_run', metavar='ANALYSISNAME', type=str, help='analysis to run', nargs='+', action='append', default=[])
#parser.add_argument('-v', dest='verbose', metavar='ANALYSISNAME', type=str, help='analysis to verify only', nargs='+', action='append', default=[])
parser.add_argument('-d', dest='disable_run', metavar='ANALYSISNAME', type=str, help='analysis to disable run', nargs='+', action='append', default=[])

args = parser.parse_args()

print args.module

import dataanalysis as da
import dataanalysis

if args.verbose:
    dataanalysis.global_log_enabled=True
    dataanalysis.global_fancy_output=True
else:
    dataanalysis.global_log_enabled=False
    dataanalysis.global_fancy_output=False


if args.quiet:
    dataanalysis.printhook.LogStream(None,lambda x:set(x)&set(['top','main']))
else:
    dataanalysis.printhook.LogStream(None,lambda x:True)

modules=[m[0] for m in args.module]

import imp,sys

for m in modules:
    print "importing",m

    sys.path.append(".")
    module,name=importing.load_by_name(m)
    globals()[name]=module



for a in args.assume:
    print "assume",a[0]
    l=eval(a[0])

    dataanalysis.AnalysisFactory.WhatIfCopy('commandline',l)

A=dataanalysis.AnalysisFactory[args.object_name]() # more better this

print A

for a in args.force_run:
    print "force run",a
    try:
        b=dataanalysis.AnalysisFactory[a[0]]() # more better this
        b.__class__.cached=False
    except: # oh now!
        pass

#for a in args.verify:
#    print "verify",a
#    b=dataanalysis.AnalysisFactory[a[0]]() # more better this
#    b.__class__.only_verify_cache_record=False

for a in args.disable_run:
    print "disable run",a
    b=dataanalysis.AnalysisFactory[a[0]]() # more better this
    b.__class__.produce_disabled=True

A.process(output_required=True)

#print "aliases:"
#for a,b in dataanalysis.AnalysisFactory.aliases:
#    print a,b

if args.plotit:
    A.plot_schema('schema.png')
