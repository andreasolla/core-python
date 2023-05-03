from ignis.executor.core.io.IJsonWriter import IJsonWriter
from ignis.executor.core.io.IReader import IReader, IReaderType
from ignis.executor.core.io.IWriter import IWriter, IWriterType, IEnumTypes


class IJsonValue:
	__slots__ = ['__value']

	def __init__(self, value=None):
		self.__value = value

	def setNull(self):
		self.__value = None

	def setValue(self, value):
		self.__value = value

	def getValue(self):
		return self.__value

	def isNull(self):
		return self.__value is None

	def isBoolean(self):
		return type(self.__value) == bool

	def isInteger(self):
		return type(self.__value) == int

	def isFloat(self):
		return type(self.__value) == float

	def isNumber(self):
		return self.isInteger() or self.isDouble()

	def isString(self):
		return type(self.__value) == str

	def isArray(self):
		return type(self.__value) == list

	def isDict(self):
		return type(self.__value) == dict

	def __str__(self):
		return self.__value.__str__()

	def __repr__(self):
		return self.__value.__repr__()


IJsonWriter[IJsonValue] = lambda o: o.getValue()


def __read_null(protocol):
	return None


def __read_boolean(protocol):
	return protocol.readBool()


def __read_integer(protocol):
	return protocol.readI64()


def __read_float(protocol):
	return protocol.readDouble()


def __read_string(protocol):
	return protocol.readString()


def __read_array(protocol):
	obj = list()
	size = IReader._readSizeAux(protocol)
	if IReader._readTypeAux(protocol) != IEnumTypes.I_JSON.value:
		raise TypeError("malformed json serialization")
	for i in range(0, size):
		readerType = __getReader(IReader._readTypeAux(protocol))
		obj.append(readerType(protocol))
	return obj


def __read_dict(protocol):
	obj = dict()
	size = IReader._readSizeAux(protocol)
	if IReader._readTypeAux(protocol) != IEnumTypes.I_STRING.value:
		raise TypeError("json dict key must be string")
	if IReader._readTypeAux(protocol) != IEnumTypes.I_JSON.value:
		raise TypeError("malformed json serialization")
	for i in range(0, size):
		key = __read_string(protocol)
		valueReader = __getReader(IReader._readTypeAux(protocol))
		obj[key] = valueReader(protocol)
	return obj


__json_reader_types = {
	IEnumTypes.I_VOID.value: __read_null,
	IEnumTypes.I_BOOL.value: __read_boolean,
	IEnumTypes.I_I64.value: __read_integer,
	IEnumTypes.I_STRING.value: __read_string,
	IEnumTypes.I_LIST.value: __read_array,
	IEnumTypes.I_MAP.value: __read_dict,
}


def __getReader(tp):
	reader = __json_reader_types.get(tp, None)
	if not reader:
		raise TypeError("malformed json serialization")
	return reader


def __reader(protocol):
	return IJsonValue(__getReader(IReader._readTypeAux(protocol))(protocol))


IReader[IEnumTypes.I_JSON] = IReaderType(IJsonValue, __reader)


def __write_null(protocol, obj):
	return None


def __write_boolean(protocol, obj):
	return protocol.writeBool(obj)


def __write_integer(protocol, obj):
	return protocol.writeI64(obj)


def __write_float(protocol, obj):
	return protocol.writeDouble(obj)


def __write_string(protocol, obj):
	return protocol.writeString(obj)


def __writeList(protocol, obj):
	size = len(obj)
	IWriter._writeSizeAux(protocol, size)
	IWriter._writeTypeAux(IEnumTypes.I_JSON.value)

	for elem in obj:
		writer = __getWriter(obj)
		IWriter._writeTypeAux(writer[0])
		writer[1](protocol, obj)


def __writeMap(protocol, obj):
	size = len(obj)
	IWriter._writeSizeAux(protocol, size)
	IWriter._writeTypeAux(IEnumTypes.I_STRING.value)
	IWriter._writeTypeAux(IEnumTypes.I_JSON.value)
	for key, value in obj.items():
		if type(key) != str:
			raise TypeError("json dict key must be string")
		__write_string(protocol, key)
		writer = __getWriter(value)
		IWriter._writeTypeAux(writer[0])
		writer[1](protocol, value)


__json_writer_types = {
	type(None): (IEnumTypes.I_VOID.value, __write_null),
	bool: (IEnumTypes.I_VOID.value, __write_boolean),
	int: (IEnumTypes.I_VOID.value, __write_integer),
	float: (IEnumTypes.I_VOID.value, __write_float),
	str: (IEnumTypes.I_VOID.value, __write_string),
	list: (IEnumTypes.I_VOID.value, __writeList),
	dict: (IEnumTypes.I_VOID.value, __writeMap),
}


def __getWriter(obj):
	writer = __json_writer_types.get(type(obj), None)
	if not writer:
		raise TypeError("json does not support " + str(type(obj)))
	return writer


def __writer(protocol, obj):
	writer = __getWriter(obj)
	IWriter._writeTypeAux(writer[0])
	writer[1](protocol, obj)


IWriter[IJsonValue] = IWriterType(IEnumTypes.I_JSON, __writer)
