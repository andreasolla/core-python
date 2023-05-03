from ignis.executor.core.io.IEnumTypes import IEnumTypes


class __IWriterAbs(type):
	__types = dict()

	@classmethod
	def __setitem__(cls, key, value):
		cls.__types[key] = value

	@classmethod
	def __getitem__(cls, key):
		return cls.__types[key]

	@classmethod
	def __delitem__(cls, key):
		cls.__types.pop(key, None)

	@classmethod
	def __contains__(cls, key):
		return key in cls.__types.keys()

	@classmethod
	def _getWriterType(cls, obj):
		try:
			return cls.__types[type(obj)].get(obj)
		except KeyError as ex:
			raise NotImplementedError("IWriterType not implemented for " + str(type(obj)))

	@classmethod
	def _getWriterTypeClass(cls, class_):
		try:
			return cls.__types[class_]
		except KeyError as ex:
			raise NotImplementedError("IWriterType not implemented for " + str(class_))

	@classmethod
	def _writeSizeAux(cls, protocol, size):
		protocol.writeI64(size)

	@classmethod
	def _writeTypeAux(cls, protocol, tp):
		protocol.writeByte(tp)

	@classmethod
	def write(cls, protocol, obj):
		writerType = cls._getWriterType(obj)
		writerType.writeType(protocol)
		writerType.write(protocol, obj)


class IWriter(metaclass=__IWriterAbs):
	pass


class IWriterType:

	def __init__(self, id, write):
		if isinstance(id, IEnumTypes):
			id = id.value
		self.__id = id
		self.write = write
		self.__types = dict()

	def __setitem__(self, key, value):
		self.__types[key] = value

	def __getitem__(self, key):
		return self.__types[key]

	def __delitem__(self, key):
		self.__types.pop(key, None)

	def __contains__(self, key):
		return key in self.__types.keys()

	def _getWriter(self, cls):
		return self.__types.get(cls, self)

	def getId(self):
		return self.__id

	def get(self, obj):
		return self

	def writeType(self, protocol):
		protocol.writeByte(self.__id)


class IWriterTypeIt(IWriterType):

	def __init__(self, id, write):
		IWriterType.__init__(self, id, write)

	def get(self, obj):
		if obj is not None and len(obj) > 0:
			return self._getWriter(type(next(iter(obj))))
		return self._getWriter(None)


IWriter[type(None)] = IWriterType(IEnumTypes.I_VOID, lambda protocol, obj: None)
IWriter[bool] = IWriterType(IEnumTypes.I_BOOL, lambda protocol, obj: protocol.writeBool(obj))
IWriter[int] = IWriterType(IEnumTypes.I_I64, lambda protocol, obj: protocol.writeI64(obj))
IWriter[float] = IWriterType(IEnumTypes.I_DOUBLE, lambda protocol, obj: protocol.writeDouble(obj))
IWriter[str] = IWriterType(IEnumTypes.I_STRING, lambda protocol, obj: protocol.writeString(obj))


def __writeList(protocol, obj):
	size = len(obj)
	IWriter._writeSizeAux(protocol, size)
	if size == 0:
		writerType = IWriter._getWriterType(None)
	else:
		writerType = IWriter._getWriterType(obj[0])
	writerType.writeType(protocol)
	for elem in obj:
		writerType.write(protocol, elem)


def __writeSet(protocol, obj):
	size = len(obj)
	IWriter._writeSizeAux(protocol, size)
	if size == 0:
		writerType = IWriter._getWriterType(None)
	else:
		writerType = IWriter._getWriterType(next(iter(obj)))
	writerType.writeType(protocol)
	for elem in obj:
		writerType.write(protocol, elem)


def __writeMap(protocol, obj):
	size = len(obj)
	IWriter._writeSizeAux(protocol, size)
	if size == 0:
		keyWriter = IWriter._getWriterType(None)
		valueWriter = IWriter._getWriterType(None)
	else:
		entry = next(iter(obj.items()))
		keyWriter = IWriter._getWriterType(entry[0])
		valueWriter = IWriter._getWriterType(entry[1])
	keyWriter.writeType(protocol)
	valueWriter.writeType(protocol)
	for key, value in obj.items():
		keyWriter.write(protocol, key)
		valueWriter.write(protocol, value)


def __writePair(protocol, obj):
	if len(obj) != 2:
		raise NotImplementedError("IWriterType not implemented for len(tuple) != 2")
	firstWriter = IWriter._getWriterType(obj[0])
	secondWriter = IWriter._getWriterType(obj[1])
	firstWriter.writeType(protocol)
	secondWriter.writeType(protocol)
	firstWriter.write(protocol, obj[0])
	secondWriter.write(protocol, obj[1])


def __writeBytes(protocol, obj):
	IWriter._writeSizeAux(protocol, len(obj))
	for b in obj:
		protocol.writeByte(b - 128)


IWriter[list] = IWriterTypeIt(IEnumTypes.I_LIST, __writeList)
IWriter[set] = IWriterTypeIt(IEnumTypes.I_SET, __writeSet)
IWriter[dict] = IWriterType(IEnumTypes.I_MAP, __writeMap)
IWriter[tuple] = IWriterType(IEnumTypes.I_PAIR, __writePair)
IWriter[bytes] = IWriterType(IEnumTypes.I_BINARY, __writeBytes)
IWriter[bytearray] = IWriterType(IEnumTypes.I_BINARY, __writeBytes)


def __writePairList(protocol, obj):
	size = len(obj)
	IWriter._writeSizeAux(protocol, size)
	if size == 0:
		firstWriter = IWriter._getWriterType(None)
		secondWriter = firstWriter
	else:
		firstWriter = IWriter._getWriterType(obj[0][0])
		secondWriter = IWriter._getWriterType(obj[0][1])
	firstWriter.writeType(protocol)
	secondWriter.writeType(protocol)
	for elem in obj:
		firstWriter.write(protocol, elem[0])
		secondWriter.write(protocol, elem[1])


class IWriterTypePairList(IWriterType):  # Pair List optimization

	def __init__(self, id, write, listWriter):
		IWriterType.__init__(self, id, write)
		self.__listWriter = listWriter

	def get(self, obj):
		if len(obj[0]) != 2:
			return self.__listWriter
		return self


IWriter[list][tuple] = IWriterTypePairList(IEnumTypes.I_PAIR_LIST, __writePairList, IWriter[list])
