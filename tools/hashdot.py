#!/bin/env python

import argparse
import ast

from dataanalysis.displaygraph import plot_hashe

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('hashefiles', metavar='hashefiles', type=str, nargs='+',
                               help='an')
    parser.add_argument('--outpng', metavar='hashefiles', type=str, help='an', default="graph.png")
    parser.add_argument('--outdot', metavar='hashefiles', type=str, help='an', default="graph.dot")
    parser.add_argument('--nuid', help='an', action='store_true', default=False)

    args = parser.parse_args()

    for hashefile in args.hashefiles:
        hashe=ast.literal_eval(open(hashefile).read())
        plot_hashe(hashe,args.outpng,dotfn=args.outdot,assign_nuids=args.nuid)
