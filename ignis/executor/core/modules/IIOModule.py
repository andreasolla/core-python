import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.IIOImpl import IIOImpl
from ignis.rpc.executor.io.IIOModule import Iface as IIOModuleIface

logger = logging.getLogger(__name__)


class IIOModule(IModule, IIOModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__impl = IIOImpl(executor_data)

	def loadClass(self, src):
		try:
			self._use_source(src)
		except Exception as ex:
			self._pack_exception(ex)

	def loadLibrary(self, path):
		try:
			self._executor_data.loadLibraryFunctions()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionCount(self):
		try:
			return len(self._executor_data.getPartitions())
		except Exception as ex:
			self._pack_exception(ex)

	def countByPartition(self):
		try:
			return [len(part) for part in self._executor_data.getPartitions()]
		except Exception as ex:
			self._pack_exception(ex)

	def partitionApproxSize(self):
		try:
			return self.__impl.partitionApproxSize()
		except Exception as ex:
			self._pack_exception(ex)

	def plainFile(self, path, delim):
		try:
			self.__impl.plainFile(path, delim=delim)
		except Exception as ex:
			self._pack_exception(ex)

	def plainFile3(self, path, minPartitions, delim):
		try:
			self.__impl.plainFile(path, minPartitions, delim)
		except Exception as ex:
			self._pack_exception(ex)

	def textFile(self, path):
		try:
			if(path.startswith("hdfs://")):
				self.__impl.hdfsTextFile(path)
			else:
				self.__impl.textFile(path)
		except Exception as ex:
			self._pack_exception(ex)

	def textFile2(self, path, minPartitions):
		try:
			self.__impl.textFile(path, minPartitions)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionObjectFile(self, path, first, partitions):
		try:
			self.__impl.partitionObjectFile(path, first, partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionObjectFile4(self, path, first, partitions, src):
		try:
			self._use_source(src)
			self.__impl.partitionObjectFile(path, first, partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionTextFile(self, path, first, partitions):
		try:
			self.__impl.partitionTextFile(path, first, partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionJsonFile4a(self, path, first, partitions, objectMapping):
		try:
			self.__impl.partitionJsonFile(path, first, partitions, objectMapping)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionJsonFile4b(self, path, first, partitions, src):
		try:
			self._use_source(src)
			self.__impl.partitionJsonFile(path, first, partitions, objectMapping=True)
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsObjectFile(self, path, compression, first):
		try:
			self.__impl.saveAsObjectFile(path, compression, first)
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsTextFile(self, path, first):
		try:
			if(path.startswith("hdfs://")):
				self.__impl.saveAsHdfsTextFile(path, first)
			else:
				self.__impl.saveAsTextFile(path, first)
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsJsonFile(self, path, first, pretty):
		try:
			self.__impl.saveAsJsonFile(path, first, pretty)
		except Exception as ex:
			self._pack_exception(ex)
