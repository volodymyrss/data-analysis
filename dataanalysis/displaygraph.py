import subprocess

import pydot


def plot_hashe(hashe,pngfn,dotfn=None,show=True):
    graph = pydot.Dot(graph_type='digraph', splines='ortho' )

    def process_hashe(hashe,graph,root=None):
        if hashe is None:
            return

        if hashe[0]=="list":
            for h in hashe[1:]:
                process_hashe(h,graph,root)

        if hashe[0]=="analysis":
            graph.add_node(pydot.Node(repr(hashe[-1]).replace(":",""), style="filled", fillcolor="green", shape="box"))

            if root is not None:
                graph.add_edge(pydot.Edge(repr(hashe[-1]).replace(":",""), repr(root)))

            process_hashe(hashe[1],graph,hashe[-1])

        if isinstance(hashe,str):
            graph.add_node(pydot.Node(repr(hashe).replace(":",""), style="filled", fillcolor="green", shape="box"))

            if root is not None:
                graph.add_edge(pydot.Edge(repr(hashe).replace(":",""), repr(root)))


    process_hashe(hashe,graph,None)
    graph.write_png(pngfn)

    if show:
        subprocess.Popen(["display",pngfn])

    if dotfn is not None:
        graph.write(dotfn)
