import os

from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol
from ignis.executor.core.storage.IRawPartition import IRawPartition
from ignis.executor.core.transport.IFileObjectTransport import IFileObjectTransport
from ignis.executor.core.transport.IHeaderTransport import IHeaderTransport
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer
from ignis.executor.core.transport.IZlibTransport import IZlibTransport


class IDiskPartition(IRawPartition):
	TYPE = "Disk"

	def __init__(self, path, compression, native, persist=False, read=False, cls=list):
		self.__path = path
		self.__header_cache = None
		self.__destroy = not persist
		with open("/dev/null", "wb") as f:
			file_trans = IFileObjectTransport(f)
			IRawPartition.__init__(self, file_trans, compression, native, cls)
			tmp = open(path, "ab")
			self._zlib.write(b'0')
			self._zlib.flush()
		self._transport.fileobj = tmp
		if read:
			with open(self.__path + ".header", "rb") as file:
				file_header = IFileObjectTransport(file)
				zlib_header = IZlibTransport(file_header)
				compatible, _ = self._readHeader(zlib_header)
				if not compatible:
					self._native = not self._native
		else:
			self.clear()

	def clone(self):
		newPath = self.__path
		i = 0
		while os.path.exists(newPath + "." + str(i)):
			i += 1
		newPath += "." + str(i)
		newPartition = IDiskPartition(newPath, self._compression, self._native, cls=self._cls)
		self.copyTo(newPartition)
		return newPartition

	def clear(self):
		IRawPartition.clear(self)
		self._transport.fileobj.truncate(0)
		self.sync()

	def fit(self):
		pass

	def type(self):
		return self.TYPE

	def bytes(self):
		return os.path.getsize(self.__path) + self._header_size

	def sync(self):
		self._writeHeader()
		with open(self.__path + ".header", "wb") as file_header:
			file_header.write(self.__header_cache)
		IRawPartition.sync(self)

	def persist(self, p):
		self.__destroy = not p

	def getPath(self):
		return self.__path

	def rename(self, new_path):
		self._transport.fileobj.close()
		error = False
		try:
			os.rename(self.__path, new_path)
			if os.path.exists(self.__path + ".header"):
				os.rename(self.__path + ".header", new_path + ".header")
			self.__path = new_path
		except OSError as ex:
			error = ValueError("error: " + new_path + " is not a valid storage name, " + str(ex))
		self._transport.fileobj = open(self.__path, "ab")
		self.sync()  # flush and create new header
		if error:
			raise error

	def __del__(self):
		if self.__destroy:
			self._transport.close()
			try:
				os.remove(self.__path)
				os.remove(self.__path + ".header")
			except:
				pass
		else:
			self.sync()

	def _writeHeader(self):
		buffer = IMemoryBuffer(IRawPartition._HEADER)
		zlib_buffer = IZlibTransport(buffer, self._compression)
		proto = IObjectProtocol(zlib_buffer)
		proto.writeSerialization(self._native)
		self._header.write(proto, self._elements, self._type)
		zlib_buffer.flush()
		self.__header_cache = buffer.getBufferAsBytes()
		self._header_size = len(self.__header_cache)

	def _readTransport(self):
		return IHeaderTransport(IFileObjectTransport(open(self.__path, "rb")), self.__header_cache)
