from thrift.transport.TTransport import TTransportBase


class IBytesTransport(TTransportBase):

	def __init__(self, bytes):
		self.__bytes = bytes
		self.__pos = 0

	def isOpen(self):
		return True

	def read(self, sz):
		consumed = min(len(self.__bytes) - self.__pos, sz)
		old_pos = self.__pos
		self.__pos += consumed
		return self.__bytes[old_pos:self.__pos]
