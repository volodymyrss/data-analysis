from distutils.core import setup

setup(
    name='data-analysis',
    version='1.0',
    py_modules=[
		"dataanalysis",
		"dataanalysis.analysisfactory",
		"dataanalysis.bcolors",
		"dataanalysis.caches",
		"dataanalysis.caches.cache_core",
		"dataanalysis.caches.sdsc",
		"dataanalysis.caches.delegating",
		"dataanalysis.caches.backends",
		"dataanalysis.core",
		"dataanalysis.hashtools",
		"dataanalysis.importing",
		"dataanalysis.printhook",
		"dataanalysis.jsonify",
		"dataanalysis.context",
		],
    scripts=['tools/rundda.py'],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
)
