import pickle


class INativeReader:
	class __Wrapper:

		def __init__(self, transport):
			self.__trans = transport

		def read(self, sz):
			return self.__trans.readAll(sz)

		def readline(self):
			buff = bytearray()
			while True:
				byte = self.__trans.read(1)
				buff.append(byte)
				if byte == b'\n':
					break
			return buff

		def readinto(self, b):
			toRead = len(b)
			data = self.read(toRead)
			toWrite = len(data)
			b[:toWrite] = data
			return toWrite

	@classmethod
	def read(cls, protocol):
		wrapper = INativeReader.__Wrapper(protocol.trans)
		return pickle.load(wrapper)
