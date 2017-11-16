from distutils.core import setup

setup(
    name='data-analysis',
    version='1.0',
    packages=["dataanalysis","dataanalysis.caches"],
    scripts=['tools/rundda.py','tools/hashdot.py'],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
)
