from distutils.core import setup

setup(
    name='data-analysis',
    version='1.1.2',
    packages=["dataanalysis","dataanalysis.caches"],
    entry_points={
            'console_scripts': [
                    'dda-emerge = dataanalysis.emerge:main',
                    'dda-run = dataanalysis.rundda:main',
                    'rundda.py = dataanalysis.rundda:main',
                    'dda-hashdot = dataanalysis.hashdot:main',
                    'hashdot.py = dataanalysis.rundda:main',
                ]
        },
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description="data analysis",
)
