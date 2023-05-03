import sys

from ignis.executor.api.IReadIterator import IReadIterator
from ignis.executor.api.IWriteIterator import IWriteIterator
from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol
from ignis.executor.core.storage.IPartition import IPartition
from ignis.executor.core.transport.IZlibTransport import IZlibTransport


class IMemoryPartition(IPartition):
    TYPE = "Memory"

    def __init__(self, native, cls=list, elements=None):
        if elements is None:
            self.__elements = cls()
        else:
            self.__elements = elements
        self.__native = native
        self.__cls = cls

    def readIterator(self):
        return IMemoryReadIterator(self.__elements)

    def __iter__(self):
        return self.__elements.__iter__()

    def writeIterator(self):
        return IMemoryWriteIterator(self.__elements)

    def read(self, transport):
        zlib_trans = IZlibTransport(transport)
        proto = IObjectProtocol(zlib_trans)
        new_elems = proto.readObject()
        if isinstance(new_elems, type(self.__elements)):
            self.__elements += new_elems
        else:
            it = self.writeIterator()
            for elem in new_elems:
                it.write(elem)

    def write(self, transport, compression=0, native=None, listHeader=True):
        if native is None:
            native = self.__native
        zlib_trans = IZlibTransport(transport, compression)
        proto = IObjectProtocol(zlib_trans)
        proto.writeObject(self.__elements, native, listHeader)
        zlib_trans.flush()

    def clone(self):
        newPartition = IMemoryPartition(self.__native, self.__cls)
        self.copyTo(newPartition)
        return newPartition

    def copyFrom(self, source):
        if type(source) == IMemoryPartition and type(self.__elements) == type(source.__elements):
            self.__elements += source.__elements
        else:
            it = source.readIterator()
            while it.hasNext():
                self.__elements.append(it.next())

    def moveFrom(self, source):
        if self.type() == source.type and len(self.__elements) == 0 and type(self.__elements) == type(
                source.__elements):
            self.__elements, source.__elements = source.__elements, self.__elements
        else:
            self.copyFrom(source)
        source.clear()

    def size(self):
        return len(self.__elements)

    def bytes(self):
        if self.size() == 0:
            return 0
        else:
            return sys.getsizeof(self.__elements[0], 1024) * self.size()

    def clear(self):
        self.__elements.clear()

    def fit(self):
        pass

    def type(self):
        return IMemoryPartition.TYPE

    def __setitem__(self, index, value):
        self.__elements[index] = value

    def __getitem__(self, index):
        return self.__elements[index]

    def _inner(self):
        return self.__elements


class IMemoryReadIterator(IReadIterator):

    def __init__(self, elements):
        self.__elements = elements
        self.__pos = 0

    def next(self):
        pos = self.__pos
        self.__pos += 1
        return self.__elements[pos]

    def hasNext(self):
        return self.__pos < len(self.__elements)


class IMemoryWriteIterator(IWriteIterator):

    def __init__(self, elements):
        self.__elements = elements

    def write(self, obj):
        self.__elements.append(obj)
