from types import FunctionType

import ignis.rpc.source.ttypes
from ignis.executor.core.ILibraryLoader import ILibraryLoader, IFunctionDef, IFunctionLambda
from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer


class ISource:

	def __init__(self, src, native=False, **kwargs):
		self.__native = native
		self.__inner = ignis.rpc.source.ttypes.ISource()
		obj = ignis.rpc.source.ttypes.IEncoded()
		if isinstance(src, str):
			obj.name = src
		elif isinstance(src, FunctionType):
			if src.__name__ == '<lambda>':
				obj.bytes = ILibraryLoader.pickle(IFunctionLambda(src))
			else:
				obj.bytes = ILibraryLoader.pickle(IFunctionDef(src))
		else:
			obj.bytes = ILibraryLoader.pickle(src)
		self.__inner.obj = obj
		for name, var in kwargs.items():
			self.addParam(name, var)

	@classmethod
	def wrap(cls, src):
		if isinstance(src, ISource):
			return src
		else:
			return ISource(src)

	def addParam(self, name, value):
		buffer = IMemoryBuffer(1000)
		proto = IObjectProtocol(buffer)
		proto.writeObject(value, self.__native)
		self.__inner.params[name] = buffer.getBufferAsBytes()
		return self

	def rpc(self):
		return self.__inner
