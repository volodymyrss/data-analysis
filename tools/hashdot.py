#!/bin/env python

import argparse
import ast

from dataanalysis.displaygraph import plot_hashe

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('hashefiles', metavar='hashefiles', type=str, nargs='+',
                               help='an')

    args = parser.parse_args()

    for hashefile in args.hashefiles:
        hashe=ast.literal_eval(open(hashefile).read())
        pngfn="hashe.png"
        plot_hashe(hashe,pngfn)
