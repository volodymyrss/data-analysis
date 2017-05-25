import pydot 
import argparse
import ast
import subprocess

def plot_hashe(hashe,pngfn,show=True):
    graph = pydot.Dot(graph_type='digraph')

    def process_hashe(hashe,graph,root=None):
        if hashe is None:
            return

        if hashe[0]=="list":
            for h in hashe[1:]:
                process_hashe(h,graph,root)

        if hashe[0]=="analysis":
            graph.add_node(pydot.Node(repr(hashe[-1]), style="filled", fillcolor="green"))

            if root is not None:
                graph.add_edge(pydot.Edge(repr(hashe[-1]), repr(root)))

            process_hashe(hashe[1],graph,hashe[-1])

    process_hashe(hashe,graph,None)
    graph.write_png(pngfn)

    if show:
        subprocess.Popen(["display",pngfn])


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('hashefiles', metavar='hashefiles', type=str, nargs='+',
                           help='an')
#parser.add_argument('--sum', dest='accumulate', action='store_const',
#                           const=sum, default=max,
#                                              help='sum the integers (default: find the max)')

args = parser.parse_args()

for hashefile in args.hashefiles:
    hashe=ast.literal_eval(open(hashefile).read())
    pngfn="hashe.png"
    plot_hashe(hashe,pngfn)
