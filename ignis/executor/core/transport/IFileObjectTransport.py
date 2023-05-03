from thrift.transport.TTransport import TFileObjectTransport


class IFileObjectTransport(TFileObjectTransport):

	def __init__(self, fileobj):
		TFileObjectTransport.__init__(self, fileobj)
		self.__close = False

	def close(self):
		self.fileobj.close()
		self.__close = True

	def __del__(self):
		if not self.__close:
			self.close()
