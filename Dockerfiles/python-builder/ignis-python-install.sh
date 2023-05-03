#!/bin/bash

export DEBIAN_FRONTEND=noninteractive
apt update
apt -y --no-install-recommends install python3.8 python3.8-distutils python3-numpy
rm -rf /var/lib/apt/lists/*

python3 ${IGNIS_HOME}/bin/get-pip.py
rm -f ${IGNIS_HOME}/bin/get-pip.py
python3 -m pip install certifi

cd ${IGNIS_HOME}/core/python-libs/
cd mpi4py
python3 setup.py install
cd ..
cd thrift
python3 setup.py install
rm -fR ${IGNIS_HOME}/core/python-libs/

cd ${IGNIS_HOME}/core/python/
python3 setup.py develop

PYTHON_LIB=$(python3 -c "import sysconfig; print(sysconfig.get_path('stdlib'))")
ln -s $PYTHON_LIB/site-packages $PYTHON_LIB/dist-packages

rm -fR ${IGNIS_HOME}/opt/go
