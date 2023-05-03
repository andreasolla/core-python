import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.IPipeImpl import IPipeImpl
from ignis.executor.core.modules.impl.IReduceImpl import IReduceImpl
from ignis.executor.core.modules.impl.IRepartitionImpl import IRepartitionImpl
from ignis.executor.core.modules.impl.ISortImpl import ISortImpl
from ignis.rpc.executor.general.IGeneralModule import Iface as IGeneralModuleIface

logger = logging.getLogger(__name__)


class IGeneralModule(IModule, IGeneralModuleIface):

    def __init__(self, executor_data):
        IModule.__init__(self, executor_data, logger)
        self.__pipe_impl = IPipeImpl(executor_data)
        self.__sort_impl = ISortImpl(executor_data)
        self.__reduce_impl = IReduceImpl(executor_data)
        self.__repartition_impl = IRepartitionImpl(executor_data)

    def executeTo(self, src):
        try:
            self.__pipe_impl.executeTo(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def map_(self, src):
        try:
            self.__pipe_impl.map(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def filter(self, src):
        try:
            self.__pipe_impl.filter(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def flatmap(self, src):
        try:
            self.__pipe_impl.flatmap(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def keyBy(self, src):
        try:
            self.__pipe_impl.keyBy(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def mapWithIndex(self, src):
        try:
            self.__pipe_impl.mapWithIndex(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def mapPartitions(self, src):
        try:
            self.__pipe_impl.mapPartitions(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def mapPartitionsWithIndex(self, src):
        try:
            self.__pipe_impl.mapPartitionsWithIndex(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def mapExecutor(self, src):
        try:
            self.__pipe_impl.mapExecutor(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def mapExecutorTo(self, src):
        try:
            self.__pipe_impl.mapExecutorTo(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def groupBy(self, src, numPartitions):
        try:
            self.__pipe_impl.keyBy(self._executor_data.loadLibrary(src))
            self.__reduce_impl.groupByKey(numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def sort(self, ascending):
        try:
            self.__sort_impl.sort(ascending)
        except Exception as ex:
            self._pack_exception(ex)

    def sort2(self, ascending, numPartitions):
        try:
            self.__sort_impl.sort(ascending, numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def sortBy(self, src, ascending):
        try:
            self.__sort_impl.sortBy(self._executor_data.loadLibrary(src), ascending)
        except Exception as ex:
            self._pack_exception(ex)

    def sortBy3(self, src, ascending, numPartitions):
        try:
            self.__sort_impl.sortBy(self._executor_data.loadLibrary(src), ascending, numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def union_(self, other, preserveOrder):
        try:
            self.__reduce_impl.union(other, preserveOrder)
        except Exception as ex:
            self._pack_exception(ex)

    def union2(self, other, preserveOrder, src):
        try:
            self._use_source(src)
            self.__reduce_impl.union(other, preserveOrder)
        except Exception as ex:
            self._pack_exception(ex)

    def join(self, other, numPartitions):
        try:
            self.__reduce_impl.join(other, numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def join3(self, other, numPartitions, src):
        try:
            self._use_source(src)
            self.__reduce_impl.join(other, numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def distinct(self, numPartitions):
        try:
            self.__reduce_impl.distinct(numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def distinct2(self, numPartitions, src):
        try:
            self._use_source(src)
            self.__reduce_impl.distinct(numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def repartition(self, numPartitions, preserveOrdering, global_):
        try:
            self.__repartition_impl.repartition(numPartitions, preserveOrdering, global_)
        except Exception as ex:
            self._pack_exception(ex)

    def partitionByRandom(self, numPartitions, seed):
        try:
            self.__repartition_impl.partitionByRandom(numPartitions, seed)
        except Exception as ex:
            self._pack_exception(ex)

    def partitionByHash(self, numPartitions):
        try:
            self.__repartition_impl.partitionByHash(numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def partitionBy(self, src, numPartitions):
        try:
            self.__repartition_impl.partitionBy(self._executor_data.loadLibrary(src), numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def flatMapValues(self, src):
        try:
            self.__pipe_impl.flatMapValues(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def mapValues(self, src):
        try:
            self.__pipe_impl.mapValues(self._executor_data.loadLibrary(src))
        except Exception as ex:
            self._pack_exception(ex)

    def groupByKey(self, numPartitions):
        try:
            self.__reduce_impl.groupByKey(numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def groupByKey2(self, numPartitions, src):
        try:
            self._use_source(src)
            self.__reduce_impl.groupByKey(numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def reduceByKey(self, src, numPartitions, localReduce):
        try:
            self.__reduce_impl.reduceByKey(self._executor_data.loadLibrary(src), numPartitions, localReduce)
        except Exception as ex:
            self._pack_exception(ex)

    def aggregateByKey(self, zero, seqOp, numPartitions):
        try:
            self.__reduce_impl.zero(self._executor_data.loadLibrary(zero))
            self.__reduce_impl.aggregateByKey(self._executor_data.loadLibrary(seqOp), numPartitions, True)
        except Exception as ex:
            self._pack_exception(ex)

    def aggregateByKey4(self, zero, seqOp, combOp, numPartitions):
        try:
            self.__reduce_impl.zero(self._executor_data.loadLibrary(zero))
            self.__reduce_impl.aggregateByKey(self._executor_data.loadLibrary(seqOp), numPartitions, False)
            self.__reduce_impl.reduceByKey(self._executor_data.loadLibrary(combOp), numPartitions, False)
        except Exception as ex:
            self._pack_exception(ex)

    def foldByKey(self, zero, src, numPartitions, localFold):
        try:
            self.__reduce_impl.zero(self._executor_data.loadLibrary(zero))
            self.__reduce_impl.foldByKey(self._executor_data.loadLibrary(src), numPartitions, localFold)
        except Exception as ex:
            self._pack_exception(ex)

    def sortByKey(self, ascending):
        try:
            self.__sort_impl.sortByKey(ascending)
        except Exception as ex:
            self._pack_exception(ex)

    def sortByKey2a(self, ascending, numPartitions):
        try:
            self.__sort_impl.sortByKey(ascending, numPartitions)
        except Exception as ex:
            self._pack_exception(ex)

    def sortByKey2b(self, src, ascending):
        try:
            self.__sort_impl.sortByKeyBy(self._executor_data.loadLibrary(src), ascending)
        except Exception as ex:
            self._pack_exception(ex)

    def sortByKey3(self, src, ascending, numPartitions):
        try:
            self.__sort_impl.sortByKeyBy(self._executor_data.loadLibrary(src), ascending, numPartitions)
        except Exception as ex:
            self._pack_exception(ex)
