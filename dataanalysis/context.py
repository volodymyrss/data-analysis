# il n'y a pas de hors-texte

from __future__ import print_function

import dataanalysis.core as da


class InContext(da.DataAnalysis):
    root=None
    leaves=None

    def get_version(self):
        v=self.get_signature()+"."+self.version
        v+=".root_%s"%self.root

        if self.leaves is not None:
            v += ".leaves_%s" % repr(self.leaves)

        return v


    def main(self):
        self.data={}

        if self.leaves is not None:
            leaves=self.leaves
        else:
            leaves=da.AnalysisFactory.cache.keys()

        self.data[self.root]={}

        for leaf_name in leaves:
            leaf=da.AnalysisFactory.get_by_name(leaf_name)
            if leaf._da_locally_complete and da.hashtools.find_object(leaf._da_locally_complete,self.root):
                print(leaf)
                self.data[self.root][leaf_name]=leaf.export_data()


