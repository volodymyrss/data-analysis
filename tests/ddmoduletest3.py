import time

from dataanalysis import core as da

import ddmoduletest2


class D2_AAnalysis(da.DataAnalysis):
    cached=True
    #cache=cache

    assumed_data=None

    def main(self):
        self.data="dataA"
        if self.assumed_data is not None:
            self.data += self.assumed_data

