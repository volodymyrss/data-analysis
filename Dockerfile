FROM docker.io/integralsw/osa-python:11.2-2-g667521a3-20220403-190332-refcat-43.0-heasoft-6.32.1-python-3.10.11

RUN export HOME_OVERRRIDE=/tmp/home && mkdir -pv /tmp/home/pfiles && \
	source /init.sh &&  \
	pip install "minio<7.0.0" rdflib-jsonld pytest-flask "mistune<2.1,>=2.0.3" && \
	cd /tmp && git clone https://github.com/ferrigno/data-analysis.git && \
	cd data-analysis &&\
	python setup.py install

