#!/bin/env python

import argparse
import ast

from dataanalysis.displaygraph import plot_hashe
    
def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('hashefiles', metavar='hashefiles', type=str, nargs='+',help='an')
    parser.add_argument('--outpng', metavar='hashefiles', type=str, help='an', default="graph.png")
    parser.add_argument('--outdot', metavar='hashefiles', type=str, help='an', default="graph.dot")
    parser.add_argument('--nuid', help='an', action='store_true', default=False)
    parser.add_argument('--filter', metavar='filternodes', type=str, nargs='+', help='an', default=[])
    parser.add_argument('--wait', metavar='wait', type=float, help='an', default=None)


    args = parser.parse_args()

    filtered_nodes = []
    for fn in args.filter:
        filtered_nodes+=fn.split(",")
    if len(filtered_nodes)==0:
        filtered_nodes=[".*"]

    for hashefile in args.hashefiles:
        hashe=ast.literal_eval(open(hashefile).read())
        plot_hashe(hashe,args.outpng,dotfn=args.outdot,assign_nuids=args.nuid,filtered_nodes=filtered_nodes,wait=args.wait)

