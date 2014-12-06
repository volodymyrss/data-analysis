data-analysis
=============

Framework to facilitate development of the (scientific) data analysis pipelines.
Designed to handle organized processing and storing results of  different stages of analysis for moderate-scale (~10Tb) data archive.

The principal idea is to organize the pipeline in blocks (classes, inheriting from DataAnalysis) without side effects. Resulf of a DataAnalysis is some data. Data is transofrmed by analysis to other data. Any Data is identified by a tree of connected DataAnalysis that where used to produce it.

Many (but not all) Data is cached: it will not be recomputed if requested, instead it will be retrieved from a storage backend (Cache). Since every DataAnalysis is a pure function of it's input, Data is uniquely characterized by the analysis graph that lead to it.

The strong points of this approach are:

* avoiding to repeat analysis: frequently used results are stored and reused (saving computing time)
* Data is be stored according to it's origin. E.g. in a nice directory structure optionally with an index (saving disk space)
* analysis is rerunnable, with a granularity of a single DataAnalysis (built-in fault tolerance)
* analysis can be easily paralelized (saving implementation time)

weak points are:

* special effort is needed to design the pipeline in the form of the pure funtions. however, there are not restrictions on the design within a single DataAnalysis. The effort to design is equivalnet to design any analysis pipeline in a reusable way.
* changing analysis graph is handled in special way.
* very large analysis is described by very complex graph


The development was driven by the needs of analysing data of INTEGRAL space observatory. 
