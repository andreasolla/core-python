import unittest

from ignis.executor.core.IExecutorData import IExecutorData
from ignis.rpc.source.ttypes import ISource, IEncoded


class ILibraryLoaderTest(unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		self.__executor_data = IExecutorData()
		props = self.__executor_data.getContext().props()
		props["ignis.executor.directory"] = "./"

	def __asSource(self, name):
		return ISource(obj=IEncoded(name=name))

	def test_loadStrLambda(self):
		context = self.__executor_data.getContext()
		result = self.__executor_data.loadLibrary(self.__asSource("lambda x: x * x")).call(4, context)
		self.assertEqual(result, 16)

		context = self.__executor_data.getContext()
		result = self.__executor_data.loadLibrary(self.__asSource("lambda x, y: x / y")).call(8, 2, context)
		self.assertEqual(result, 4)

	def test_loadStrFunction(self):
		context = self.__executor_data.getContext()
		result = self.__executor_data.loadLibrary(self.__asSource("def f(x, c):\n  return x * x")).call(4, context)
		self.assertEqual(result, 16)

		context = self.__executor_data.getContext()
		result = self.__executor_data.loadLibrary(self.__asSource("def f(x, y, c):\n  return x / y")).call(8, 2, context)
		self.assertEqual(result, 4)

	def test_loadStrSource(self):
		src = """
from ignis.executor.api.function.IFunction import IFunction

class Test(IFunction):

	def call(self, v, context):
		return v * v

__ignis_library__ = ["Test"]
		"""
		self.__executor_data.loadLibraryFunctions(src)
		self.__executor_data.loadLibraryFunctions(src)

		context = self.__executor_data.getContext()
		result = self.__executor_data.loadLibrary(self.__asSource("Test")).call(4, context)
		self.assertEqual(result, 16)
