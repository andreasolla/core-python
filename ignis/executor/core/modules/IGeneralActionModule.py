import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.IPipeImpl import IPipeImpl
from ignis.executor.core.modules.impl.IReduceImpl import IReduceImpl
from ignis.executor.core.modules.impl.ISortImpl import ISortImpl
from ignis.rpc.executor.general.action.IGeneralActionModule import Iface as IGeneralActionModuleIface

logger = logging.getLogger(__name__)


class IGeneralActionModule(IModule, IGeneralActionModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__pipe_impl = IPipeImpl(executor_data)
		self.__sort_impl = ISortImpl(executor_data)
		self.__reduce_impl = IReduceImpl(executor_data)

	def execute(self, src):
		try:
			self.__pipe_impl.execute(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def reduce(self, src):
		try:
			self.__reduce_impl.reduce(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def treeReduce(self, src):
		try:
			self.__reduce_impl.treeReduce(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def collect(self):
		try:
			pass # Do nothing
		except Exception as ex:
			self._pack_exception(ex)

	def aggregate(self, zero, seqOp, combOp):
		try:
			self.__reduce_impl.zero(self._executor_data.loadLibrary(zero))
			self.__reduce_impl.aggregate(self._executor_data.loadLibrary(seqOp))
			self.__reduce_impl.reduce(self._executor_data.loadLibrary(combOp))
		except Exception as ex:
			self._pack_exception(ex)

	def treeAggregate(self, zero, seqOp, combOp):
		try:
			self.__reduce_impl.zero(self._executor_data.loadLibrary(zero))
			self.__reduce_impl.aggregate(self._executor_data.loadLibrary(seqOp))
			self.__reduce_impl.treeReduce(self._executor_data.loadLibrary(combOp))
		except Exception as ex:
			self._pack_exception(ex)

	def fold(self, zero, src):
		try:
			self.__reduce_impl.zero(self._executor_data.loadLibrary(zero))
			self.__reduce_impl.fold(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def treeFold(self, zero, src):
		try:
			self.__reduce_impl.zero(self._executor_data.loadLibrary(zero))
			self.__reduce_impl.treeFold(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def take(self, num):
		try:
			self.__pipe_impl.take(num)
		except Exception as ex:
			self._pack_exception(ex)

	def foreach_(self, src):
		try:
			self.__pipe_impl.foreach(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def foreachPartition(self, src):
		try:
			self.__pipe_impl.foreachPartition(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def foreachExecutor(self, src):
		try:
			self.__pipe_impl.foreachExecutor(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def top(self, num):
		try:
			self.__sort_impl.top(num)
		except Exception as ex:
			self._pack_exception(ex)

	def top2(self, num, comp):
		try:
			self.__sort_impl.top(num, self._executor_data.loadLibrary(comp))
		except Exception as ex:
			self._pack_exception(ex)

	def takeOrdered(self, num):
		try:
			self.__sort_impl.takeOrdered(num)
		except Exception as ex:
			self._pack_exception(ex)

	def takeOrdered2(self, num, comp):
		try:
			self.__sort_impl.takeOrdered(num, self._executor_data.loadLibrary(comp))
		except Exception as ex:
			self._pack_exception(ex)

	def keys(self):
		try:
			self.__pipe_impl.keys()
		except Exception as ex:
			self._pack_exception(ex)

	def values(self):
		try:
			self.__pipe_impl.values()
		except Exception as ex:
			self._pack_exception(ex)
