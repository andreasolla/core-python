from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol
from ignis.executor.core.storage.IRawPartition import IRawPartition
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer


class IRawMemoryPartition(IRawPartition):
	TYPE = "RawMemory"

	def __init__(self, bytes, compression, native, cls=list):
		buffer = IMemoryBuffer(bytes + IRawPartition._HEADER)
		IRawPartition.__init__(self, buffer, compression, native, cls)
		self.clear()

	def clone(self):
		newPartition = IRawMemoryPartition(self.bytes(), self._compression, self._native, self._cls)
		self.copyTo(newPartition)
		return newPartition

	def bytes(self):
		sz = self._transport.getBufferSize() - IRawPartition._HEADER + self._header_size
		return max(sz, 0)

	def clear(self):
		IRawPartition.clear(self)
		self._transport.resetBuffer()
		self.sync()

	def fit(self):
		self._transport.setBufferSize(self._transport.getBufferSize())

	def type(self):
		return self.TYPE

	def _getBuffer(self):
		return self._transport

	def sync(self):
		IRawPartition.sync(self)
		self._writeHeader()

	def _readTransport(self):
		init = IRawPartition._HEADER - self._header_size
		rbuffer = IMemoryBuffer(buf=self._transport.getBuffer(), sz=self._transport.writeEnd())
		rbuffer.setReadBuffer(init)
		return rbuffer

	def _writeHeader(self):
		proto = IObjectProtocol(self._zlib)
		self._zlib.reset()
		write_pos = self._transport.writeEnd()
		self._transport.resetBuffer()

		proto.writeSerialization(self._native)
		self._header.write(proto, self._elements, self._type)
		self._zlib.flush()
		# Align header to the left
		self._header_size = self._transport.writeEnd()
		header = self._transport.readAll(self._header_size)
		self._transport.setReadBuffer(0)
		self._transport.setWriteBuffer(IRawPartition._HEADER - self._header_size)
		self._transport.write(header)
		self._transport.setWriteBuffer(max(write_pos, IRawPartition._HEADER))
