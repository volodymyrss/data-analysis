from setuptools import setup

setup(
    name='data-analysis',
    version='1.0.0',
    packages=["dataanalysis","dataanalysis.caches"],
    entry_points={
            'console_scripts': [
                    #'dda-emerge = dataanalysis.emerge:main',
     #               'dda-run = dataanalysis.rundda:main',
     #               'rundda.py = dataanalysis.rundda:main',
     #               'dda-hashdot = dataanalysis.hashdot:main',
     #               'hashdot.py = dataanalysis.rundda:main',
                ]
        },
    scripts=[
                'tools/rundda.py',
                'tools/hashdot.py',
            ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
)
