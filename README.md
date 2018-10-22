data-analysis
=============
[![Build Status](https://travis-ci.org/volodymyrss/data-analysis.svg?branch=master)](https://travis-ci.org/volodymyrss/data-analysis)
[![codebeat badge](https://codebeat.co/badges/be1fafc7-ebdc-4fdd-8f60-18b1630c85bc)](https://codebeat.co/projects/github-com-volodymyrss-data-analysis-master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/a84b843c73fd4395b72ac00c8738a46c)](https://www.codacy.com/app/vladimir.savchenko/data-analysis?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=volodymyrss/data-analysis&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/a84b843c73fd4395b72ac00c8738a46c)](https://www.codacy.com/app/vladimir.savchenko/data-analysis?utm_source=github.com&utm_medium=referral&utm_content=volodymyrss/data-analysis&utm_campaign=Badge_Coverage)
[![Requirements Status](https://requires.io/github/volodymyrss/data-analysis/requirements.svg?branch=master)](https://requires.io/github/volodymyrss/data-analysis/requirements/?branch=master)

Framework facilitating semantic declarative expression of reproducible data analysis
workflows.  

Workflow is expressed as collection of "pure function" single-valued Analysis Nodes, represented as __DataAnalysis__ classes.
Python class inheritance is used to define __rdfs:subClassOf__ relations, and the class attributes induce __rdf:Property__ defining OWL-compatible ontology.

The workflow definition is compatible with __CWL__ workflow expression (complete implementation of the integration is in progress). 

The results are stored in an __append-only database__ indexed with the data __provenance__, derived directly from the workflow definition.

Below is an example of a workflow definition:

```python
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
```


The framework also provides different possibilities for retrieving values of the function: evaluating, restoring from cache, delegating in a queue (with a simple example queue implementation) or to a remote resource (e.g. http service).
The Analysis Nodes may be also deployed in a __Function-as-a-Service__ infrastructure (and then queried as remote resources).

The framework was originally designed to handle organized processing and
storing results of different stages [INTEGRAL](http://sci.esa.int/integral/) scientific data analysis workflow. The archive is moderate scale (tens of Tb), but contains highly diverse data, complicating archiving in relatinal databases.
The framework is intended for ingesting and processing new data in a append-only NoSQL database.

Many (but not all) Data is *cached*: it will not be recomputed if
requested, instead it will be retrieved from a storage backend
(Cache). Since every DataAnalysis is a pure function of it's input,
Data is uniquely characterized by the workflow DAG that lead to its
production. *Caching* is the only means of storing data, and is decoupled from the execution.

The strong points of this approach are:

* avoiding repeating analysis: frequently used results are stored and reused (saving computing time)
* Data is be stored according to it's provenance (origin). This allows to naturally partition data according to the provenance. Seeing storage as caching of workflow evaluation allows to naturally replicate and re-use analysis results. (saving disk space)
* analysis is rerunnable, with a granularity of a single DataAnalysis (built-in fault tolerance)
* analysis can be easily paralelized (saving implementation time)

The workflow expression is designed to be easy to use and re-use, constructing workflow node namespace from a sequence of modules. 
At the time of the execution, each DataAnalysis Node is provided with the neccessary inputs by the means of dependecy injection.

weak points are:

* special effort is needed to design the pipeline in the form of the pure funtions. however, there are not restrictions on the design within a single DataAnalysis. One can consider that this effort is equivalent to design any analysis pipeline in a way that allows easy and controlled reuse of diverse data.
* analysis graph can be changed as a result of the analysis. This process may be confusing, and is addressed with analogue of higher order functions. 
* very large analysis may be eventually described by a very large graph. Shortcuts and aliases for parts of the graph are designed and can be used to avoid this.

The development was driven by the needs of analysing data of INTEGRAL space observatory: as of 2015 it is 20 Tb in 20Mfiles, about 1000 different kinds of data (see https://github.com/volodymyrss/dda-ddosa/).

