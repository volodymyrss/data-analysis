import subprocess

import pydot
import time
import hashtools


def plot_hashe(hashe,pngfn,dotfn=None,show=True,assign_nuids=False,filter_nodes=None, assign_status=False):
    graph = pydot.Dot(graph_type='digraph', splines='ortho' )

    def process_hashe(hashe,graph,root=None):
        if hashe is None:
            return

        if hashe[0]=="list":
            for h in hashe[1:]:
                process_hashe(h,graph,root)

        if hashe[0]=="analysis":
            node_label=repr(hashe[-1]).replace(":","")
            if assign_nuids:
                nuid=hashtools.shhash(hashe)[:8]
                node_label+="_"+nuid

            graph.add_node(pydot.Node(node_label, style="filled", fillcolor="green", shape="box"))

            if root is not None:
                graph.add_edge(pydot.Edge(node_label, repr(root)))

            process_hashe(hashe[1],graph,node_label)

        if isinstance(hashe,str):
            graph.add_node(pydot.Node(repr(hashe).replace(":",""), style="filled", fillcolor="green", shape="box"))

            if root is not None:
                graph.add_edge(pydot.Edge(repr(hashe).replace(":",""), repr(root)))


    process_hashe(hashe,graph,None)
    graph.write_png(pngfn)

    if show:
        p=subprocess.Popen(["display",pngfn])
        time.sleep(0.5)
        p.kill()


    if dotfn is not None:
        graph.write(dotfn)
