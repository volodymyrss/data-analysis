

import dataanalysis
import dataanalysis.core as da

from dataanalysis.hashtools import shhash, hashe_replace_object, hashe_map


class AnyAnalysis(da.DataAnalysis):
    def main(self):
        raise Exception("requested to run ANY analysis:"+repr(self.__class__))

class Factorize(da.DataAnalysis):
    root="undefined"
    leaves=[]

    def get_version(self):
        v=self.get_signature()+"."+self.version
        v+=".Factor_%s"%self.root
        for leaf in self.leaves:
            v += ".By_%s" % leaf

        return v


    run_for_hashe = True
    #allow_alias = True

    def main(self):
        abstract_assumptions=[]

        for leaf in self.leaves:
            aa=self.factory.get_by_name(leaf).clone()
            aa.abstract = True
            aa.run_for_hashe = False
            aa.allow_alias = False

            for k in dir(aa):
                if k.startswith("input_"):
                    print(("replacing input",aa,getattr(aa,k),"with AnyAnalysis"))
                    setattr(aa,k,AnyAnalysis)

            print(aa)
            abstract_assumptions.append(aa)

        mf = self.factory.get_by_name(self.root).__class__(
                assume=abstract_assumptions
            )

        ahash = mf.process(output_required=False, run_if_haveto=False)[0]

        print(("generalized hash:", ahash))
        print(("replaced None hash:",hashe_replace_object(ahash, None, 'None') ))
        rh = shhash(ahash)
        print(("hashmapped:", hashe_map(rh, str)))
        print(("reduced hash", rh))
        handle = dataanalysis.DataHandle(self.get_version()+'.processing_definition.' + rh[:8])

        self.factory.note_factorization(dict(
            origin_object=self.__class__.__name__,
            origin_module=__name__,
            generalized_hash=ahash,
            reduced_hash=rh,
            handle=handle.handle,
        ))
        return [handle]

