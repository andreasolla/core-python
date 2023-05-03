import time

from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.protocol.TMultiplexedProtocol import TMultiplexedProtocol
from thrift.transport.TSocket import TSocket, logger as socket_logger
from thrift.transport.TZlibTransport import TZlibTransport

from ignis.rpc.driver.backend import IBackendService
from ignis.rpc.driver.cluster import IClusterService
from ignis.rpc.driver.dataframe import IDataFrameService
from ignis.rpc.driver.properties import IPropertiesService
from ignis.rpc.driver.worker import IWorkerService


class IClient:

	def __init__(self, port, compression):
		self.__transport = TZlibTransport(TSocket("localhost", port), compression)
		socket_logger.disabled = True  # Avoid reconnection errors
		for i in range(0, 10):
			try:
				self.__transport.open()
				break
			except Exception as ex:
				if i == 9:
					raise ex
				time.sleep(i)
		socket_logger.disabled = False
		protocol = TCompactProtocol(self.__transport)
		self.__backendService = IBackendService.Client(TMultiplexedProtocol(protocol, "IBackend"))
		self.__propertiesService = IPropertiesService.Client(TMultiplexedProtocol(protocol, "IProperties"))
		self.__clusterService = IClusterService.Client(TMultiplexedProtocol(protocol, "ICluster"))
		self.__workerService = IWorkerService.Client(TMultiplexedProtocol(protocol, "IWorker"))
		self.__dataframeService = IDataFrameService.Client(TMultiplexedProtocol(protocol, "IDataFrame"))

	def getBackendService(self):
		return self.__backendService

	def getPropertiesService(self):
		return self.__propertiesService

	def getClusterService(self):
		return self.__clusterService

	def getWorkerService(self):
		return self.__workerService

	def getDataFrameService(self):
		return self.__dataframeService

	def _close(self):
		self.__transport.close()
