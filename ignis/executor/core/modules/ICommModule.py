import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.ICommImpl import ICommImpl
from ignis.rpc.executor.comm.ICommModule import Iface as ICommModuleIface

logger = logging.getLogger(__name__)


class ICommModule(IModule, ICommModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__impl = ICommImpl(executor_data)

	def openGroup(self):
		try:
			return self.__impl.openGroup()
		except Exception as ex:
			self._pack_exception(ex)

	def closeGroup(self):
		try:
			self.__impl.closeGroup()
		except Exception as ex:
			self._pack_exception(ex)

	def joinToGroup(self, id, leader):
		try:
			self.__impl.joinToGroup(id, leader)
		except Exception as ex:
			self._pack_exception(ex)

	def joinToGroupName(self, id, leader, name):
		try:
			self.__impl.joinToGroupName(id, leader, name)
		except Exception as ex:
			self._pack_exception(ex)

	def hasGroup(self, name):
		try:
			return self.__impl.hasGroup(name)
		except Exception as ex:
			self._pack_exception(ex)

	def destroyGroup(self, name):
		try:
			self.__impl.destroyGroup(name)
		except Exception as ex:
			self._pack_exception(ex)

	def destroyGroups(self):
		try:
			self.__impl.destroyGroups()
		except Exception as ex:
			self._pack_exception(ex)

	def getProtocol(self):
		try:
			return self.__impl.getProtocol()
		except Exception as ex:
			self._pack_exception(ex)

	def getPartitions(self, protocol):
		try:
			return self.__impl.getPartitions(protocol)
		except Exception as ex:
			self._pack_exception(ex)

	def getPartitions2(self, protocol, minPartitions):
		try:
			return self.__impl.getPartitions(protocol, minPartitions)
		except Exception as ex:
			self._pack_exception(ex)

	def setPartitions(self, partitions):
		try:
			self.__impl.setPartitions(partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def setPartitions2(self, partitions, src):
		try:
			# src in python can be empty
			if src.obj.name != "" or src.obj.bytes is not None:
				self._use_source(src)
			self.__impl.setPartitions(partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def driverGather(self, group, src):
		try:
			# src in python is empty
			self.__impl.driverGather(group)
		except Exception as ex:
			self._pack_exception(ex)

	def driverGather0(self, group, src):
		try:
			# src in python is empty
			self.__impl.driverGather0(group)
		except Exception as ex:
			self._pack_exception(ex)

	def driverScatter(self, group, partitions):
		try:
			self.__impl.driverScatter(group, partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def driverScatter3(self, group, partitions, src):
		try:
			if not self._executor_data.hasPartitions():
				self._use_source(src)
			self.__impl.driverScatter(group, partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def importData(self, group, source, threads):
		try:
			self.__impl.importData(group, source, threads)
		except Exception as ex:
			self._pack_exception(ex)

	def importData4(self, group, source, threads, src):
		try:
			self._use_source(src)
			self.__impl.importData(group, source, threads)
		except Exception as ex:
			self._pack_exception(ex)
