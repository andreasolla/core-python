from thrift.transport.TTransport import TTransportBase


class IHeaderTransport(TTransportBase):

	def __init__(self, trans, header):
		self.__trans = trans
		self.__header = header
		self.__pos = 0

	def isOpen(self):
		return True

	def open(self):
		self.__trans.open()

	def close(self):
		self.__trans.close()

	def read(self, sz):
		consumed = min(len(self.__header) - self.__pos, sz)
		old_pos = self.__pos
		self.__pos += consumed
		if len(self.__header) - self.__pos == 0:
			self.read = self.__trans.read
			if consumed == 0:
				return self.__trans.read(sz)
		return self.__header[old_pos:self.__pos]
