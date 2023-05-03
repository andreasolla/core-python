import os

from ignis.executor.core.IExecutorData import IExecutorData
from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl
from ignis.rpc.source.ttypes import ISource, IEncoded


class IModuleTest:

    def __init__(self):
        self._executor_data = IExecutorData()
        self._library = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                                     "TestFunctions.py")
        props = self._executor_data.getContext().props()
        props["ignis.transport.compression"] = "0"
        props["ignis.partition.compression"] = "0"
        props["ignis.partition.serialization"] = "native"
        props["ignis.executor.directory"] = "./"
        props["ignis.executor.cores"] = "1"
        props["ignis.transport.cores"] = "0"
        props["ignis.modules.load.type"] = "false"
        props["ignis.modules.exchange.type"] = "sync"

    def newSource(self, name):
        return ISource(obj=IEncoded(name=self._library + ":" + name))

    def rankVector(self, elems):
        n = int(len(elems) / self._executor_data.getContext().executors())
        rank = self._executor_data.getContext().executorId()
        return elems[n * rank: n * (rank + 1)]

    def loadToPartitions(self, elems, partitions):
        group = self._executor_data.getPartitionTools().newPartitionGroup(partitions)
        self._executor_data.setPartitions(group)
        part_size = int(len(elems) / partitions)
        for p in range(partitions):
            writer = group[p].writeIterator()
            i = part_size * p
            while i < part_size * (p + 1) and i < len(elems):
                writer.write(elems[i])
                i += 1

    def getFromPartitions(self):
        elems = list()
        group = self._executor_data.getPartitions()
        for p in range(len(group)):
            reader = group[p].readIterator()
            while reader.hasNext():
                elems.append(reader.next())
        return elems

    def _normalize(self, e):
        if isinstance(e, str):
            return ''.join(sorted(e))
        else:
            return e