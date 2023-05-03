import logging
import threading

from ignis.driver.api.IDriverException import IDriverException
from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.storage import IMemoryPartition
from ignis.rpc.executor.cachecontext.ICacheContextModule import Iface as ICacheContextModuleIface

logger = logging.getLogger(__name__)


class IDriverContext(IModule, ICacheContextModuleIface):

    def __init__(self, executor_data):
        IModule.__init__(self, executor_data, logger)
        self.__lock = threading.Lock()
        self.__next_id = 0
        self.__context = dict()
        self.__data = dict()

    def __getContext(self, id):
        value = self.__context.get(id, None)
        if value is None:
            raise ValueError("context " + str(id) + " not found")
        del self.__context[id]
        return value

    def saveContext(self):
        try:
            with self.__lock:
                id = self.__next_id
                self.__next_id += 1
                self.__context[id] = self._executor_data.getPartitions()
                return id
        except Exception as ex:
            self._pack_exception(ex)

    def clearContext(self):
        try:
            with self.__lock:
                self._executor_data.deletePartitions()
                self._executor_data.clearVariables()
        except Exception as ex:
            self._pack_exception(ex)

    def loadContext(self, id):
        try:
            with self.__lock:
                self._executor_data.setPartitions(self.__getContext(id))
        except Exception as ex:
            self._pack_exception(ex)

    def loadContextAsVariable(self, id, name):
        raise self._pack_exception(RuntimeError("Driver does not implement loadContextAsVariable"))

    def cache(self, id, level):
        raise self._pack_exception(RuntimeError("Driver does not implement cache"))

    def loadCache(self, id):
        try:
            with self.__lock:
                value = self.__data.get(id, None)
                if value is None:
                    raise ValueError("data " + str(id) + " not found")
                self._executor_data.setPartitions(value())
        except Exception as ex:
            self._pack_exception(ex)

    def parallelize(self, data, native):
        try:

            def get():
                group = self._executor_data.getPartitionTools().newPartitionGroup()

                if isinstance(data, bytes):
                    partition = IMemoryPartition(native, cls=bytearray, elements=bytearray(data))
                elif isinstance(data, bytearray):
                    partition = IMemoryPartition(native, cls=bytearray, elements=data)
                elif isinstance(data, list):
                    partition = IMemoryPartition(native, cls=list, elements=data)
                elif data.__class__.__name__ == 'ndarray':
                    from ignis.executor.core.io.INumpy import INumpyWrapper as Wrapper
                    class INumpyWrapper(Wrapper):
                        def __init__(self):
                            Wrapper.__init__(self, len(data), data.dtype)

                    partition = IMemoryPartition(native=native, cls=INumpyWrapper, elements=Wrapper(array=data))
                else:
                    partition = IMemoryPartition(native)
                    it = partition.writeIterator()
                    for item in data:
                        it.write(item)

                group.add(partition)
                return group

            with self.__lock:
                id = self.__next_id
                self.__next_id += 1
                self.__data[id] = get
                return id
        except Exception as ex:
            raise IDriverException(str(ex)) from ex

    def collect(self, id):
        try:
            with self.__lock:
                group = self.__getContext(id)

            if self._executor_data.getPartitionTools().isMemory(group):
                cls = group[0]._IMemoryPartition__cls
                if len(group) > 1:
                    result = IMemoryPartition(False, cls=cls)
                    for part in group:
                        part.copyTo(result)
                else:
                    result = group[0]
                elems = result._IMemoryPartition__elements
                if cls.__name__ == 'INumpyWrapper':
                    return elems.array
                return elems

            result = list()
            for part in group:
                it = part.readIterator()
                while it.hasNext():
                    result.append(it.next())
            return result
        except Exception as ex:
            raise IDriverException(str(ex)) from ex

    def collect1(self, id):
        l = self.collect(id)[0]
        if len(l) == 0:
            raise IDriverException("Empty collection")
        return l[0]
