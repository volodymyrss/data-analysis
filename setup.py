from distutils.core import setup

setup(
    name='data-analysis',
    version='1.0',
    py_modules=["analysisfactory", "backends", "bcolors", "caches", "dataanalysis", "hashtools", "importing", "printhook", "setup"],
    scripts=['tools/rundda.py'],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
)
