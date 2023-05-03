from ignis.executor.api.IReadIterator import IReadIterator
from ignis.executor.api.IWriteIterator import IWriteIterator
from ignis.executor.core.io.IEnumTypes import IEnumTypes
from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol, INativeWriter, IWriter, INativeReader, IReader
from ignis.executor.core.storage.IPartition import IPartition
from ignis.executor.core.transport.IZlibTransport import IZlibTransport


class IRawPartition(IPartition):
	_HEADER = 30

	def __init__(self, transport, compression, native, cls):
		self._transport = transport
		self._zlib = IZlibTransport(transport, compression)
		self._compression = compression
		self._native = native
		self._elements = 0
		self._type = None
		self._header = None
		self._header_size = 0
		self._cls = cls
		IRawPartition.clear(self)

	def readIterator(self):
		self.sync()
		zlib_it = IZlibTransport(self._readTransport())
		proto = IObjectProtocol(zlib_it)
		native = proto.readSerialization()
		self._header.read(proto)
		return IRawReadIterator(proto, self)

	def writeIterator(self):
		if self._header_size == 0:
			self._writeHeader()
		return IRawWriteIterator(IObjectProtocol(self._zlib), self)

	def read(self, transport):
		zlib_in = IZlibTransport(transport)
		current_elems = self._elements
		compatible, reader = self._readHeader(zlib_in)
		self.sync()

		if compatible:
			while True:
				bb = zlib_in.read(256)
				if not bb:
					break
				self._zlib.write(bb)
		else:
			it = self.writeIterator()
			proto_buffer = IObjectProtocol(zlib_in)
			current_elems, self._elements = self._elements, current_elems
			while self._elements < current_elems:
				it.write(reader(proto_buffer))

	def write(self, transport, compression=0, native=None):
		if native is None:
			native = self._native
		self.sync()
		source = self._readTransport()
		if self._native == native:
			if compression == self._compression:
				while True:
					bb = source.read(256)
					if not bb:
						break
					transport.write(bb)
				transport.flush()
			else:
				zlib = IZlibTransport(source)
				zlib_out = IZlibTransport(transport, compression)
				while True:
					bb = zlib.read(256)
					if not bb:
						break
					zlib_out.write(bb)
				zlib_out.flush()
		else:
			zlib_out = IZlibTransport(transport, compression)
			proto_out = IObjectProtocol(zlib_out)
			proto_out.writeSerialization(native)

			if self._elements:
				if not native:
					it = self.readIterator()
					first = it.next()
					writer = IWriter._getWriterTypeClass(self._cls)
					writer = writer._getWriter(type(first))
					header = IHeader._getHeaderTypeId(writer.getId(), False)
					write, tp = header.getElemWrite(first)
					header.write(proto_out, self._elements, tp)
					write(proto_out, first)
					while it.hasNext():
						write(proto_out, it.next())
				else:
					header = IHeader._getHeaderTypeId(self._type, True)
					write, tp = header.getElemWrite(None)
					header.write(proto_out, self._elements, tp)
					it = self.readIterator()
					while it.hasNext():
						write(proto_out, it.next())
			else:
				header = IHeader._getHeaderTypeId(self._type, self._native)
				header.write(proto_out, 0, self._type)
			zlib_out.flush()

	def clone(self):
		pass

	def copyFrom(self, source):
		if (source.type() == "Disk" or source.type() == "RawMemory") and source._native == self._native:
			source.sync()
			self.sync()
			if not self._elements:
				self._type = source._type
				self._header = source._header
				self._header_size = source._header_size
			# Direct copy
			if source._compression == self._compression:
				self._elements += source._elements
				source_buffer = source._readTransport()
				source_buffer.read(source._header_size)  # skip header
				while True:
					bb = source_buffer.read(256)
					if not bb:
						break
					self._transport.write(bb)

			else:
				# Read header to initialize zlib
				source_buffer = source._readTransport()
				source_zlib = IZlibTransport(source_buffer)
				source_proto = IObjectProtocol(source_zlib)
				native = source_proto.readSerialization()
				self._readHeader(source_zlib)
				while True:
					bb = source_zlib.read(256)
					if not bb:
						break
					self._zlib.write(bb)
		else:
			writer = self.writeIterator()
			reader = source.readIterator()
			while reader.hasNext():
				writer.write(reader.next())

	def moveFrom(self, source):
		self.copyFrom(source)
		source.clear()

	def size(self):
		return self._elements

	def bytes(self):
		pass

	def clear(self):
		self._elements = 0
		self._type = IEnumTypes.I_VOID.value
		self._header = IHeader._getHeaderTypeId(IWriter._getWriterTypeClass(self._cls).getId(), self._native)

	def fit(self):
		pass

	def type(self):
		pass

	def sync(self):
		if self._elements > 0:
			self._zlib.flush()

	def _readTransport(self):
		pass

	def _readHeader(self, transport):
		proto = IObjectProtocol(transport)
		native = proto.readSerialization()
		compatible = self._native == native
		header = IHeader._getHeaderType(proto, native)
		elem, tp = header.read(proto, header_tp=False)
		if self._elements > 0:
			if tp != self._type and compatible:
				raise ValueError("Ignis serialization does not support heterogeneous basic types")
		else:
			self._header = header
			self._type = tp
		self._elements += elem
		return compatible, header.getElemRead(tp)

	def _writeHeader(self):
		pass


class IHeaderType:

	def read(self, protocol, header_tp=None):
		pass

	def write(self, protocol, elems, tp):
		pass

	def getElemRead(self, tp):
		pass

	def getElemWrite(self, obj):
		pass


class IHeaderTypeList(IHeaderType):

	def read(self, protocol, header_tp=None):
		if header_tp is None:
			header_tp = IReader._readTypeAux(protocol)
		elems = IReader._readSizeAux(protocol)
		return elems, IReader._readTypeAux(protocol)

	def write(self, protocol, elems, tp):
		IWriter._writeTypeAux(protocol, IEnumTypes.I_LIST.value)
		IWriter._writeSizeAux(protocol, elems)
		IWriter._writeTypeAux(protocol, tp)

	def getElemRead(self, tp):
		return IReader._getReaderType(tp).read

	def getElemWrite(self, obj):
		writer = IWriter._getWriterType(obj)
		return writer.write, writer.getId()


class IHeaderTypePairList(IHeaderType):

	def read(self, protocol, header_tp=None):
		if header_tp is None:
			header_tp = IReader._readTypeAux(protocol)
		elems = IReader._readSizeAux(protocol)
		return elems, (IReader._readTypeAux(protocol), IReader._readTypeAux(protocol))

	def write(self, protocol, elems, tp):
		IWriter._writeTypeAux(protocol, IEnumTypes.I_PAIR_LIST.value)
		IWriter._writeSizeAux(protocol, elems)
		IWriter._writeTypeAux(protocol, tp[0])
		IWriter._writeTypeAux(protocol, tp[1])

	def getElemRead(self, tp):
		r1 = IReader._getReaderType(tp[0]).read
		r2 = IReader._getReaderType(tp[1]).read

		def read(protocol):
			return r1(protocol), r2(protocol)

		return read

	def getElemWrite(self, obj):
		w1 = IWriter._getWriterType(obj[0])
		w2 = IWriter._getWriterType(obj[1])

		def write(protocol, obj):
			w1.write(protocol, obj[0])
			w2.write(protocol, obj[1])

		return write, (w1.getId(), w2.getId())


class IHeaderTypeBinary(IHeaderType):

	def read(self, protocol, header_tp=None):
		if header_tp is None:
			header_tp = IReader._readTypeAux(protocol)
		elems = IReader._readSizeAux(protocol)
		return elems, IEnumTypes.I_I08.value

	def write(self, protocol, elems, tp):
		IWriter._writeTypeAux(protocol, IEnumTypes.I_BINARY.value)
		IWriter._writeSizeAux(protocol, elems)

	def getElemRead(self, tp):
		return lambda protocol: protocol.readByte() + 128

	def getElemWrite(self, obj):
		return lambda protocol, obj: protocol.writeByte(obj - 128), IEnumTypes.I_I08.value


class IHeaderTypeNative(IHeaderType):

	def read(self, protocol, header_tp=None):
		protocol.readBool()  # list header
		return IReader._readSizeAux(protocol), None

	def write(self, protocol, elems, tp):
		protocol.writeBool(True)
		IWriter._writeSizeAux(protocol, elems)

	def getElemRead(self, tp):
		return INativeReader.read

	def getElemWrite(self, obj):
		return INativeWriter.write, None


class __IHeaderAbs(type):
	__types = dict()
	__native = IHeaderTypeNative()

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
		del cls.__types[key]

	@classmethod
	def _getHeaderTypeId(cls, id, native):
		if native:
			return cls.__native
		try:
			return cls.__types[id]
		except KeyError as ex:
			raise NotImplementedError("IHeaderType not implemented for id " + str(id))

	@classmethod
	def _getHeaderType(cls, protocol, native):
		if native:
			return cls.__native
		try:
			return cls.__types[IReader._readTypeAux(protocol)]
		except KeyError as ex:
			raise NotImplementedError("IHeaderType not implemented for id " + str(id))


class IHeader(metaclass=__IHeaderAbs):
	pass


IHeader[IEnumTypes.I_LIST] = IHeaderTypeList()
IHeader[IEnumTypes.I_PAIR_LIST] = IHeaderTypePairList()
IHeader[IEnumTypes.I_BINARY] = IHeaderTypeBinary()


class IRawReadIterator(IReadIterator):

	def __init__(self, protocol, partition):
		self.__protocol = protocol
		self.__partition = partition
		self.__pos = 0
		self.__read = partition._header.getElemRead(partition._type)

	def next(self):
		self.__pos += 1
		return self.__read(self.__protocol)

	def hasNext(self):
		return self.__pos < self.__partition._elements


class IRawWriteIterator(IWriteIterator):

	def __init__(self, protocol, partition):
		self.__protocol = protocol
		self.__partition = partition
		self.__write = None

	def __fast_write(self, obj):
		self.__partition._elements += 1
		self.__write(self.__protocol, obj)

	def write(self, obj):
		if self.__partition._native:
			self.__partition._type = None
			self.__partition._header = IHeader._getHeaderTypeId(self.__partition._type, True)
			self.__write, _ = self.__partition._header.getElemWrite(obj)
		else:
			writer = IWriter._getWriterTypeClass(self.__partition._cls)
			writer = writer._getWriter(type(obj))
			header = IHeader._getHeaderTypeId(writer.getId(), False)
			self.__write, type_id = header.getElemWrite(obj)
			if self.__partition._elements > 0 and self.__partition._type != type_id:
				raise ValueError("Ignis serialization does not support heterogeneous basic types")
			self.__partition._type = type_id
			self.__partition._header = header
		self.__fast_write(obj)
		self.write = self.__fast_write
