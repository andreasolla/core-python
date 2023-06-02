
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}common-builder${TAG}
ARG RELPATH=""

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y --no-install-recommends install \
		python3.8 \
		python3.8-dev \
		python3.8-distutils && \
	rm -rf /var/lib/apt/lists/* && \
	wget https://bootstrap.pypa.io/get-pip.py -O ${IGNIS_HOME}/bin/get-pip.py && \
	python3 ${IGNIS_HOME}/bin/get-pip.py

ENV MPI4PY_VERSION=3.1.1
RUN mkdir ${IGNIS_HOME}/core/python-libs && \
	cd ${IGNIS_HOME}/core/python-libs && \
	wget https://github.com/mpi4py/mpi4py/releases/download/${MPI4PY_VERSION}/mpi4py-${MPI4PY_VERSION}.tar.gz && \
	tar -xvf mpi4py-${MPI4PY_VERSION}.tar.gz && \
	rm -f mpi4py-${MPI4PY_VERSION}.tar.gz && \
	mv mpi4py-${MPI4PY_VERSION} mpi4py && \
	cd mpi4py && \
	export CC=${IGNIS_DEVEL}/mpi/bin/mpicc && \
	export CXX=${IGNIS_DEVEL}/mpi/bin/mpic++ && \
	export LIBRARY_PATH=${IGNIS_DEVEL}/mpi/lib && \
	export CPATH=${IGNIS_DEVEL}/mpi/include && \
	sed  "s/libraries():/libraries():\n    return []/g" -i setup.py && \
	if [ $(echo "${MPICH_VERSION}"  | cut -c1,2) = "3." ]; then \
		sed -re "s/^(\s+)([^\s].*MPI_Comm_connect.*)$/\1if root != rank: port_name = asmpistr(port_name, \&cportname)\n\1\2/g" -i src/mpi4py/MPI/Comm.pyx ; \
	fi && \
	python3 setup.py build  --mpicc=${IGNIS_DEVEL}/mpi/bin/mpicc

RUN cd ${IGNIS_HOME}/core/python-libs && \
	mkdir thrift && \
	cd thrift && \
	cp -R ${IGNIS_DEVEL}/thrift/lib/py/* . && \
	python3 setup.py build

COPY ${RELPATH}ignis-python-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/ignis-python-install.sh && \
	{ \
		echo '#!/bin/bash'; \
		echo 'export PYTHONHASHSEED=1234'; \
		echo 'exec python3 "${IGNIS_HOME}/core/python/ignis/Main.py" "$@"'; \
	} > ${IGNIS_HOME}/bin/ignis-python  && chmod +x ${IGNIS_HOME}/bin/ignis-python

COPY / ${IGNIS_HOME}/core/python

ADD https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz hadoop-3.3.1.tar.gz
RUN tar -xzvf hadoop-3.3.1.tar.gz && \
    cp -R hadoop-3.3.1 ${IGNIS_HOME}/core/python/hdfs/ && \
	rm -fR hadoop-3.3.1.tar.gz

COPY ${RELPATH}hdfsExplorer/go.mod /tmp/go/
COPY ${RELPATH}hdfsExplorer/go.sum /tmp/go/
COPY ${RELPATH}hdfsExplorer/hdfsExplorer.go /tmp/go/

ENV GO_VERSION=go1.20.4
RUN cd ${IGNIS_HOME} && \
	mkdir -p opt/go && \
	cd opt/go && \
	(wget https://go.dev/dl/${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz -O golang.tar.gz || \ 
	wget https://go.dev/dl/${GO_VERSION}.linux-$(uname -m).tar.gz -O golang.tar.gz) && \
	tar -xvzf  golang.tar.gz && \
	rm -fR golang.tar.gz
ENV PATH=$PATH:/${IGNIS_HOME}/opt/go/go/bin
RUN cd /tmp/go/ &&\
    go build -o libhdfsExplorer.so -buildmode=c-shared hdfsExplorer.go && \
    cp libhdfsExplorer.so ${IGNIS_HOME}/core/python/

