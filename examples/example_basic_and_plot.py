import numpy as np
import pandas as pd

import dataanalysis.core as da
from dataanalysis import displaygraph

class Events(da.DataAnalysis):
    pass

class H1D(da.DataAnalysis):
    pass

class DataUnit(da.DataAnalysis):
    def main(self):
        self.unitid="unit1"
        self.ndata = 10

class EnergyCalibrationDB(da.DataAnalysis):
    version="v1"

    def main(self):
        self.gain=2.

class RawEvents(Events):
    input_dataunit=DataUnit

    cached=True

    def main(self):
        self.events=pd.DataFrame()
        self.events['channel']=np.arange(self.input_dataunit.ndata)

        fn="event_file.txt"
        self.events.to_csv(fn)
        self.event_file=da.DataFile(fn)

class CalibratedEvents(Events):
    input_rawevents=RawEvents
    input_ecaldb=EnergyCalibrationDB

    def main(self):
        self.events=pd.DataFrame()
        self.events['energy']=self.input_rawevents.events['channel']/self.input_ecaldb.gain

class BinnedEvents(H1D):
    input_events=CalibratedEvents

    binsize=2

    def main(self):
        self.histogram=np.histogram(self.input_events.events['energy'])


if __name__ == '__main__':
    binned_events=BinnedEvents().get()
    displaygraph.plot_hashe(binned_events._da_locally_complete, "test.png")
