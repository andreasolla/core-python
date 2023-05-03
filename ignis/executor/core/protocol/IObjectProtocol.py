import collections.abc
from struct import pack, unpack

from thrift.protocol.TCompactProtocol import TCompactProtocol, CompactType, makeZigZag

from ignis.executor.core.io.INativeReader import INativeReader
from ignis.executor.core.io.INativeWriter import INativeWriter
from ignis.executor.core.io.IReader import IReader
from ignis.executor.core.io.IWriter import IWriter


class IObjectProtocol(TCompactProtocol):
	IGNIS_PROTOCOL = 0
	PYTHON_PROTOCOL = 2

	def __init__(self, trans):
		TCompactProtocol.__init__(self, trans)

	def writeBool(self, bool):
		if bool:
			self._TCompactProtocol__writeByte(CompactType.TRUE)
		else:
			self._TCompactProtocol__writeByte(CompactType.FALSE)

	def readBool(self):
		return self._TCompactProtocol__readByte() == CompactType.TRUE

	writeByte = TCompactProtocol._TCompactProtocol__writeByte
	writeI16 = TCompactProtocol._TCompactProtocol__writeI16

	def writeI32(self, i32):
		self._TCompactProtocol__writeVarint(makeZigZag(i32, 32))

	def writeI64(self, i64):
		self._TCompactProtocol__writeVarint(makeZigZag(i64, 64))

	def writeDouble(self, dub):
		self.trans.write(pack('<d', dub))

	writeBinary = TCompactProtocol._TCompactProtocol__writeBinary

	readByte = TCompactProtocol._TCompactProtocol__readByte
	readI16 = TCompactProtocol._TCompactProtocol__readZigZag
	readI32 = TCompactProtocol._TCompactProtocol__readZigZag
	readI64 = TCompactProtocol._TCompactProtocol__readZigZag

	def readDouble(self):
		buff = self.trans.readAll(8)
		val, = unpack('<d', buff)
		return val

	readBinary = TCompactProtocol._TCompactProtocol__readBinary

	def readObject(self):
		native = self.readSerialization()
		if native:
			header = self.readBool()
			if header:
				elems = IReader._readSizeAux(self)
				array = list()
				for i in range(0, elems):
					array.append(INativeReader.read(self))
				return array
			else:
				return INativeReader.read(self)
		else:
			return IReader.read(self)

	def writeObject(self, obj, native=False, listHeader=False):
		self.writeSerialization(native)
		if native:
			isList = isinstance(obj, (collections.abc.Sized, collections.abc.Iterable))
			self.writeBool(isList and listHeader)  # Header
			if isList and listHeader:
				IWriter._writeSizeAux(self, len(obj))
				for i in range(0, len(obj)):
					INativeWriter.write(self, obj[i])
			else:
				INativeWriter.write(self, obj)
		else:
			IWriter.write(self, obj)

	def readSerialization(self):
		id = self.readByte()
		if id == IObjectProtocol.IGNIS_PROTOCOL:
			return False
		elif id == IObjectProtocol.PYTHON_PROTOCOL:
			return True
		else:
			raise TypeError("Serialization is not compatible with Python")

	def writeSerialization(self, native):
		if native:
			self.writeByte(IObjectProtocol.PYTHON_PROTOCOL)
		else:
			self.writeByte(IObjectProtocol.IGNIS_PROTOCOL)
