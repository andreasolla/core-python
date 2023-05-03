import logging
import os
import shutil
from pathlib import Path

from ignis.executor.core.storage import IMemoryPartition, IRawMemoryPartition, IDiskPartition, IPartitionGroup

logger = logging.getLogger(__name__)


def _newNumpyMemoryPartition(sz, native, dtype):
    from ignis.executor.core.io.INumpy import INumpyWrapper as Wrapper
    def INumpyWrapper(array=None):
        return Wrapper(sz, dtype, array)
    return IMemoryPartition(native=native, cls=INumpyWrapper)


class IPartitionTools:

    def __init__(self, propertyParser, context):
        self.__properties = propertyParser
        self.__context = context
        self.__partition_id_gen = 0

    def __preferredClass(self):
        return self.__context.vars().get('STORAGE_CLASS', list)

    def swap(self, p1, p2):
        p1.__dict__, p2.__dict__ = p2.__dict__, p1.__dict__

    def newPartition(self, tp=None):
        if isinstance(tp, str):
            partitionType = tp
            other = None
        else:
            other = tp
            partitionType = self.__properties.partitionType()

        if partitionType is None:
            partitionType = self.__properties.partitionType()
        if partitionType == IMemoryPartition.TYPE:
            return self.newMemoryPartition(other.size()) if other else self.newMemoryPartition()
        elif partitionType == IRawMemoryPartition.TYPE:
            return self.newRawMemoryPartition(other.bytes()) if other else self.newRawMemoryPartition()
        elif partitionType == IDiskPartition.TYPE:
            return self.newDiskPartition()
        else:
            raise ValueError("unknown partition type: " + partitionType)

    def newPartitionGroup(self, partitions=0):
        group = IPartitionGroup()
        if isinstance(partitions, int):
            for i in range(0, partitions):
                group.add(self.newPartition())
        else:
            for p in partitions:
                group.add(self.newPartition(p))
        return group

    def newMemoryPartition(self, elems=1000, cls=None):
        if cls is not None:
            return IMemoryPartition(native=self.__properties.nativeSerialization(),
                                    cls=cls)
        if self.__preferredClass().__name__ == 'ndarray':
            return _newNumpyMemoryPartition(elems,
                                            self.__properties.nativeSerialization(),
                                            self.__context.vars()['STORAGE_CLASS_DTYPE'])
        return IMemoryPartition(native=self.__properties.nativeSerialization(),
                                cls=self.__preferredClass())

    def newNumpyMemoryPartition(self, dtype, elems=1000):
        return _newNumpyMemoryPartition(elems, self.__properties.nativeSerialization(), dtype)

    def newBytearrayMemoryPartition(self, elems=1000):
        return IMemoryPartition(native=self.__properties.nativeSerialization(),
                                cls=bytearray)

    def newRawMemoryPartition(self, sz=10 * 1024 * 1024, compression=None):
        if compression is None:
            compression = self.__properties.partitionCompression()
        return IRawMemoryPartition(bytes=sz,
                                   compression=compression,
                                   native=self.__properties.nativeSerialization(),
                                   cls=self.__preferredClass())

    def __diskPath(self, name):
        path = self.__properties.executorDirectory() + "/partitions"
        self.createDirectoryIfNotExists(path)
        if name == '':
            path += "/partition"
            path += str(self.__context.executorId())
            path += "."
            path += str(self.__partition_id_gen)
            self.__partition_id_gen += 1
        else:
            path += '/' + name
        return path

    def newDiskPartition(self, name='', persist=False, read=False):
        path = self.__diskPath(name)

        return IDiskPartition(path=path,
                              compression=self.__properties.partitionCompression(),
                              native=self.__properties.nativeSerialization(),
                              persist=persist,
                              read=read,
                              cls=self.__preferredClass())

    def copyDiskPartition(self, path, name="", persist=False):
        cmp = self.__properties.partitionCompression()
        newPath = self.__diskPath(name)
        if os.path.exists(newPath):
            os.remove(newPath)
        if os.path.exists(newPath + ".header"):
            os.remove(newPath + ".header")
        try:
            os.link(path, newPath)
            os.link(path + ".header", newPath + ".header")
        except NotImplementedError as ex:
            logger.warning("current file system not support hard links, disk partitions will be copied")
            shutil.copy(path, newPath)
            shutil.copy(path + ".header", newPath + ".header")

        return IDiskPartition(path=newPath,
                              compression=cmp,
                              native=self.__properties.nativeSerialization(),
                              persist=persist,
                              read=True,
                              cls=self.__preferredClass())

    def isMemory(self, part):
        if isinstance(part, IPartitionGroup):
            return len(part) > 0 and part[0].type() == IMemoryPartition.TYPE
        return IMemoryPartition.TYPE == part.type()

    def isRawMemory(self, part):
        if isinstance(part, IPartitionGroup):
            return len(part) > 0 and part[0] == IRawMemoryPartition.TYPE
        return IRawMemoryPartition.TYPE == part.type()

    def isDisk(self, part):
        if isinstance(part, IPartitionGroup):
            return len(part) > 0 and part[0] == IDiskPartition.TYPE
        return IDiskPartition.TYPE == part.type()

    def createDirectoryIfNotExists(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
