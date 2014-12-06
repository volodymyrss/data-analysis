data-analysis
=============

Framework to facilitate development of the (scientific) data analysis pipelines.
Designed to handle storing results of many different stages of analysis for moderate-scale (~10Tb) data archive.

The principal idea is to organize the pipeline in blocks (classes, inheriting from DataAnalysis) without side effects. Resulf of a DataAnalysis is some data. Data is transofrmed by analysis to other data. Any Data is identified by a tree of connected DataAnalysis that where used to produce it.

Many (but not all) Data is cached: it will not be recomputed if requested, instead it will be retrieved from a storage backend (Cache). Because every DataAnalysis is a pure function of it's input, Data is uniquely characterized by the analysis graph that lead to it.

The strong points of this approach are:

* avoiding repeated analysis: frequently used results are stored and reused
* Data can be stored according to it's origin. E.g. in a nice directory structure optionally with an index
* analysis is rerunnable, with a granularity of a single DataAnalysis
* analysis can be easily paralellized


weak points are:

* special effort is needed to design the pipeline in the form of the pure funtions. however, there are not restrictions on the design within a single DataAnalysis
* changing analysis graph handled specially
* very large analysis is described by very complex graph



