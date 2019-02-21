# il n'y a pas de hors-texte



import dataanalysis.core as da


class InContext(da.DataAnalysis):
    input_root=None
    input_leaves=None

    include_class_attributes=True

    def main(self):
        self.data={}

        if len(self.input_leaves)!=0:
            leaves=self.input_leaves
        else:
            leaves=list(da.AnalysisFactory.cache.keys()) # no


        root=da.AnalysisFactory.get_by_name(self.input_root.get_signature())

        self.data[self.input_root.get_signature()]={}

        for leaf in leaves:
            leaf_name=leaf.get_signature()
            if leaf._da_locally_complete and da.hashtools.find_object(leaf._da_locally_complete,root.get_version()):
                print(leaf)
                self.data[self.input_root.get_signature()][leaf_name]=leaf.export_data(
                        include_class_attributes=self.include_class_attributes,
                    )


