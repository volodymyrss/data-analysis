data-analysis
=============
[![Build Status](https://travis-ci.org/volodymyrss/data-analysis.png)](https://travis-ci.org/volodymyrss/data-analysis)[![codebeat badge](https://codebeat.co/badges/be1fafc7-ebdc-4fdd-8f60-18b1630c85bc)](https://codebeat.co/projects/github-com-volodymyrss-data-analysis-master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/a84b843c73fd4395b72ac00c8738a46c)](https://www.codacy.com/app/vladimir.savchenko/data-analysis?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=volodymyrss/data-analysis&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/a84b843c73fd4395b72ac00c8738a46c)](https://www.codacy.com/app/vladimir.savchenko/data-analysis?utm_source=github.com&utm_medium=referral&utm_content=volodymyrss/data-analysis&utm_campaign=Badge_Coverage)
[![Requirements Status](https://requires.io/github/volodymyrss/data-analysis/requirements.svg?branch=master)](https://requires.io/github/volodymyrss/data-analysis/requirements/?branch=master)

Framework to facilitate development of the (scientific) data analysis
pipelines.  Originally designed to handle organized processing and
storing results of different stages of analysis for moderate-scale 
(tens of Tb) archive of very diverse data.  It is intended for ingesting and processing new
data that are appended to existing data rather than overwriting
them. State is determined from the natural ordering of the data.

The principal idea is to organize the pipeline in analysis units
(classes, inheriting from DataAnalysis) without side effects. Result
of a DataAnalysis is some Data. Data is transofrmed by analysis to
other data. Any Data is identified by a tree of connected DataAnalysis
that where used to produce it.

Many (but not all) Data is cached: it will not be recomputed if
requested, instead it will be retrieved from a storage backend
(Cache). Since every DataAnalysis is a pure function of it's input,
Data is uniquely characterized by the analysis graph that lead to its
production.

The strong points of this approach are:

* avoiding repeating analysis: frequently used results are stored and reused (saving computing time)
* Data is be stored according to it's origin. For example in a nice directory structure optionally with an index (saving disk space)
* analysis is rerunnable, with a granularity of a single DataAnalysis (built-in fault tolerance)
* analysis can be easily paralelized (saving implementation time)

The implementation is designed to be easy to use. Each DataAnalysis is
provided with the neccessary inputs by the means of dependecy injection.

weak points are:

* special effort is needed to design the pipeline in the form of the pure funtions. however, there are not restrictions on the design within a single DataAnalysis. One can consider that this effort is equivalnet to design any analysis pipeline in a way that allows easy and controlled reuse of diverse data.
* analysis graph can be changed as a result of the analysis. This process may be confusing for those not familiar with higher order functions and functional programming. The framework implements perhaps a good way to make this process easy to intuitively understand.  
* very large analysis may be eventually described by a very large graph. Natural shortcuts and aliases for parts of the graph are designed and can be used to avoid this.


The development was driven by the needs of analysing data of INTEGRAL space observatory: as of 2015 it is 20 Tb in 20Mfiles, about 1000 different kinds of data.
