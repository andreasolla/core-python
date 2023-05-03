import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.ICacheImpl import ICacheImpl
from ignis.rpc.executor.cachecontext.ICacheContextModule import Iface as ICacheContextModuleIface

logger = logging.getLogger(__name__)


class ICacheContextModule(IModule, ICacheContextModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__impl = ICacheImpl(executor_data)

		try:
			self._executor_data.reloadLibraries()
			# load partition cache when the executor has previously crashed
			self.__impl.loadCacheFromDisk()
		except Exception as ex:
			logger.error(ex)

	def saveContext(self):
		try:
			return self.__impl.saveContext()
		except Exception as ex:
			self._pack_exception(ex)

	def clearContext(self):
		try:
			self.__impl.clearContext()
		except Exception as ex:
			self._pack_exception(ex)

	def loadContext(self, id):
		try:
			self.__impl.loadContext(id)
		except Exception as ex:
			self._pack_exception(ex)

	def loadContextAsVariable(self, id, name):
		try:
			self.__impl.loadContextAsVariable(id, name)
		except Exception as ex:
			self._pack_exception(ex)

	def cache(self, id, level):
		try:
			self.__impl.cache(id, level)
		except Exception as ex:
			self._pack_exception(ex)

	def loadCache(self, id):
		try:
			self.__impl.loadCache(id)
		except Exception as ex:
			self._pack_exception(ex)
