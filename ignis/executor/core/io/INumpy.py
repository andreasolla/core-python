import numpy

from ignis.executor.core.io.IReader import IReader, IEnumTypes, IReaderType
from ignis.executor.core.io.IWriter import IWriter

__NUMPY_TYPES = {
	IEnumTypes.I_BOOL: numpy.bool_,
	IEnumTypes.I_I08: numpy.int8,
	IEnumTypes.I_I16: numpy.int16,
	IEnumTypes.I_I32: numpy.int32,
	IEnumTypes.I_I64: numpy.int64,
	IEnumTypes.I_DOUBLE: numpy.float64
}

__LIST_READER = None


class INumpyWrapper:

	def __init__(self, sz=None, dtype=None, array=None):
		if array is None:
			self.array = numpy.empty(shape=sz, dtype=dtype)
			self.__next = 0
		else:
			self.array = array
			self.__next = len(array)

	def usedArray(self):
		return self.array[0:self.__next]

	def __len__(self):
		return self.__next

	def __getitem__(self, item):
		return self.array.__getitem__(item)

	def __setitem__(self, key, value):
		self.array[key] = value

	def __iter__(self):
		return self.array[0:self.__next].__iter__()

	def __repr__(self):
		return str(self.array)

	def __add__(self, other):
		new_elems = len(other)
		sz = self.array.size
		while new_elems > sz:
			sz = int(sz * 1.5)
		self.array.resize(sz, refcheck=False)
		self.array[self.__next:self.__next + new_elems] = other.array[0:new_elems]
		self.__next += new_elems
		return self

	def _resize(self, n):
		if n != len(self.array):
			self.array.resize(n, refcheck=False)
		self.__next = n

	def bytes(self):
		return self.array.itemsize * self.__next

	def itemsize(self):
		return self.array.itemsize

	def append(self, obj):
		if self.array.size == self.__next:
			self.array.resize(int(self.array.size * 1.5), refcheck=False)
		self.array[self.__next] = obj
		self.__next += 1

	def clear(self):
		self.__next = 0


def __readList(protocol):
	size = IReader._readSizeAux(protocol)
	tp = IReader._readTypeAux(protocol)
	readerType = IReader._getReaderType(tp)
	numpy_tp = __NUMPY_TYPES.get(tp, None)
	if numpy_tp is None:
		obj = list()
		for i in range(0, size):
			obj.append(readerType.read(protocol))
	else:
		obj = numpy.zeros(shape=size, dtype=numpy_tp)
		for i in range(0, size):
			obj[i] = readerType.read(protocol)
	return obj


def enable():
	IWriter[numpy.ndarray] = IWriter[list]
	IWriter[INumpyWrapper] = IWriter[numpy.ndarray]
	IWriter[numpy.bool_] = IWriter[bool]
	IWriter[numpy.int8] = IWriter[int]
	IWriter[numpy.uint8] = IWriter[int]
	IWriter[numpy.int16] = IWriter[int]
	IWriter[numpy.uint16] = IWriter[int]
	IWriter[numpy.int32] = IWriter[int]
	IWriter[numpy.uint32] = IWriter[int]
	IWriter[numpy.int64] = IWriter[int]
	IWriter[numpy.uint64] = IWriter[int]
	IWriter[numpy.float16] = IWriter[float]
	IWriter[numpy.float32] = IWriter[float]
	IWriter[numpy.float64] = IWriter[float]

	if IEnumTypes.I_LIST in IReader:
		global __LIST_READER
		__LIST_READER = IReader[IEnumTypes.I_LIST]
	IReader[IEnumTypes.I_LIST] = IReaderType(numpy.ndarray, __readList)


def disable():
	del IWriter[numpy.ndarray]
	del IWriter[INumpyWrapper]
	del IWriter[numpy.bool_]
	del IWriter[numpy.int8]
	del IWriter[numpy.uint8]
	del IWriter[numpy.int16]
	del IWriter[numpy.uint16]
	del IWriter[numpy.int32]
	del IWriter[numpy.uint32]
	del IWriter[numpy.int64]
	del IWriter[numpy.uint64]
	del IWriter[numpy.float16]
	del IWriter[numpy.float32]
	del IWriter[numpy.float64]

	if IEnumTypes.I_LIST in IReader and IReader[IEnumTypes.I_LIST].getClass() == numpy.ndarray:
		if __LIST_READER:
			IReader[IEnumTypes.I_LIST] = __LIST_READER
		else:
			del IReader[IEnumTypes.I_LIST]


enable()
