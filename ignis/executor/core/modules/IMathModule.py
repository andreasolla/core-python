import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.IMathImpl import IMathImpl
from ignis.executor.core.modules.impl.IReduceImpl import IReduceImpl
from ignis.executor.core.modules.impl.ISortImpl import ISortImpl
from ignis.rpc.executor.math.IMathModule import Iface as IMathModuleIface

logger = logging.getLogger(__name__)


class IMathModule(IModule, IMathModuleIface):

    def __init__(self, executor_data):
        IModule.__init__(self, executor_data, logger)
        self.__math_impl = IMathImpl(executor_data)
        self.__sort_impl = ISortImpl(executor_data)
        self.__reduce_impl = IReduceImpl(executor_data)

    def sample(self, withReplacement, num, seed):
        try:
            self.__math_impl.sample(withReplacement, num, seed)
        except Exception as ex:
            self._pack_exception(ex)

    def count(self):
        try:
            return self.__math_impl.count()
        except Exception as ex:
            self._pack_exception(ex)

    def max(self):
        try:
            self.__sort_impl.max()
        except Exception as ex:
            self._pack_exception(ex)

    def min(self):
        try:
            self.__sort_impl.min()
        except Exception as ex:
            self._pack_exception(ex)

    def max1(self, cmp):
        try:
            self.__sort_impl.max(self._executor_data.loadLibrary(cmp))
        except Exception as ex:
            self._pack_exception(ex)

    def min1(self, cmp):
        try:
            self.__sort_impl.min(self._executor_data.loadLibrary(cmp))
        except Exception as ex:
            self._pack_exception(ex)

    def sampleByKey(self, withReplacement, fractions, seed):
        try:
            self._executor_data.loadParameters(fractions)
            numPartitions = self.__math_impl.sampleByKeyFilter()
            self.__reduce_impl.groupByKey(numPartitions)
            self.__math_impl.sampleByKey(withReplacement, seed)
        except Exception as ex:
            self._pack_exception(ex)

    def countByKey(self):
        try:
            self.__math_impl.countByKey()
        except Exception as ex:
            self._pack_exception(ex)

    def countByValue(self):
        try:
            self.__math_impl.countByValue()
        except Exception as ex:
            self._pack_exception(ex)
