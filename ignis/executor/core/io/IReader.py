from ignis.executor.core.io.IEnumTypes import IEnumTypes


class __IReaderAbs(type):
	__types = dict()

	@classmethod
	def __setitem__(cls, key, value):
		if isinstance(key, IEnumTypes):
			key = key.value
		cls.__types[key] = value

	@classmethod
	def __getitem__(cls, key):
		if isinstance(key, IEnumTypes):
			key = key.value
		return cls.__types[key]

	@classmethod
	def __delitem__(cls, key):
		if isinstance(key, IEnumTypes):
			key = key.value
		cls.__types.pop(key, None)

	@classmethod
	def __contains__(cls, key):
		if isinstance(key, IEnumTypes):
			key = key.value
		return key in cls.__types.keys()

	@classmethod
	def _getReaderType(cls, id):
		try:
			return cls.__types[id]
		except KeyError as ex:
			raise NotImplementedError("IWriterType not implemented for id " + str(id))

	@classmethod
	def _readSizeAux(cls, protocol):
		return protocol.readI64()

	@classmethod
	def _readTypeAux(cls, protocol):
		return protocol.readByte()

	@classmethod
	def read(cls, protocol):
		readerType = cls._getReaderType(cls._readTypeAux(protocol))
		return readerType.read(protocol)


class IReader(metaclass=__IReaderAbs):
	pass


class IReaderType:

	def __init__(self, cls, read):
		self.__cls = cls
		self.read = read

	def getClass(self):
		return self.__cls


IReader[IEnumTypes.I_VOID] = IReaderType(type(None), lambda protocol: None)
IReader[IEnumTypes.I_BOOL] = IReaderType(bool, lambda protocol: protocol.readBool())
IReader[IEnumTypes.I_I08] = IReaderType(int, lambda protocol: protocol.readByte() + 128)
IReader[IEnumTypes.I_I16] = IReaderType(int, lambda protocol: protocol.readI16())
IReader[IEnumTypes.I_I32] = IReaderType(int, lambda protocol: protocol.readI32())
IReader[IEnumTypes.I_I64] = IReaderType(int, lambda protocol: protocol.readI64())
IReader[IEnumTypes.I_DOUBLE] = IReaderType(float, lambda protocol: protocol.readDouble())
IReader[IEnumTypes.I_STRING] = IReaderType(str, lambda protocol: protocol.readString())


def __readList(protocol):
	obj = list()
	size = IReader._readSizeAux(protocol)
	readerType = IReader._getReaderType(IReader._readTypeAux(protocol))
	for i in range(0, size):
		obj.append(readerType.read(protocol))
	return obj


def __readSet(protocol):
	obj = set()
	size = IReader._readSizeAux(protocol)
	readerType = IReader._getReaderType(IReader._readTypeAux(protocol))
	for i in range(0, size):
		obj.add(readerType.read(protocol))
	return obj


def __readMap(protocol):
	obj = dict()
	size = IReader._readSizeAux(protocol)
	keyReader = IReader._getReaderType(IReader._readTypeAux(protocol))
	valueReader = IReader._getReaderType(IReader._readTypeAux(protocol))
	for i in range(0, size):
		key = keyReader.read(protocol)
		obj[key] = valueReader.read(protocol)
	return obj


def __readPair(protocol):
	firstReader = IReader._getReaderType(IReader._readTypeAux(protocol))
	secondReader = IReader._getReaderType(IReader._readTypeAux(protocol))
	return firstReader.read(protocol), secondReader.read(protocol)


def __readBinary(protocol):
	obj = bytearray()
	size = IReader._readSizeAux(protocol)
	for i in range(0, size):
		obj.append(protocol.readByte() + 128)
	return obj


def __readPairList(protocol):
	obj = list()
	size = IReader._readSizeAux(protocol)
	firstReader = IReader._getReaderType(IReader._readTypeAux(protocol))
	secondReader = IReader._getReaderType(IReader._readTypeAux(protocol))
	for i in range(0, size):
		obj.append((firstReader.read(protocol), secondReader.read(protocol)))
	return obj


IReader[IEnumTypes.I_LIST] = IReaderType(list, __readList)
IReader[IEnumTypes.I_SET] = IReaderType(set, __readSet)
IReader[IEnumTypes.I_MAP] = IReaderType(dict, __readMap)
IReader[IEnumTypes.I_PAIR] = IReaderType(tuple, __readPair)
IReader[IEnumTypes.I_BINARY] = IReaderType(bytearray, __readBinary)
IReader[IEnumTypes.I_PAIR_LIST] = IReaderType(list, __readPairList)
