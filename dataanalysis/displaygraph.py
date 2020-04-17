import time
import re
import subprocess

import pydot

from . import hashtools


def dotify_hashe(hashe,assign_nuids=False,filtered_nodes=None,wait=None,graph=None,return_root=False):
    if graph is None:
        graph = pydot.Dot(graph_type='digraph', splines='ortho' )

    def node_filter(node_label):
        if filtered_nodes is None:
            return True

        for one_filter in filtered_nodes:
            if re.search(one_filter,node_label): return True
        return False

    def process_hashe(hashe,graph,root=None,edge_list=None):
        if edge_list is None:
            edge_list=[]

        if hashe is None:
            return

        if hashe[0]=="list":
            for h in hashe[1:]:
                process_hashe(h,graph,root,edge_list)

        if hashe[0]=="analysis":
            node_label=str(hashe[-1]).replace(":","")
            if assign_nuids:
                nuid=hashtools.shhash(hashe)[:8]
                node_label += "\nNUID=" + nuid

            if node_filter(node_label):
                graph.add_node(pydot.Node(node_label, style="filled", fillcolor="green", shape="box"))
                if root is not None and (node_label, root) not in edge_list:
                    graph.add_edge(pydot.Edge(node_label, root))
                    edge_list.append((node_label, root))

                process_hashe(hashe[1],graph,node_label,edge_list)
            else:
                process_hashe(hashe[1], graph, root,edge_list)

        if isinstance(hashe,str):
            node_label = hashe.replace(":", "")

            if node_filter(node_label):
                graph.add_node(pydot.Node(node_label, style="filled", fillcolor="green", shape="box"))
                if root is not None and (node_label, root) not in edge_list:
                    graph.add_edge(pydot.Edge(node_label, root))
                    edge_list.append((node_label, root))


    process_hashe(hashe,graph,None)

    if return_root:
        return graph,str(hashe[-1]).replace(":","")
    else:
        return graph


def plot_hashe(hashe,pngfn,dotfn=None,show=True,assign_nuids=False,filtered_nodes=None,wait=None):
    graph=dotify_hashe(hashe,assign_nuids,filtered_nodes,wait)
    graph.write_png(pngfn)

    if dotfn is not None:
        graph.write(dotfn)

    if show:
        p=subprocess.Popen(["display",pngfn])
        if wait is not None:
            time.sleep(wait)
            p.kill()
