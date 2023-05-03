import unittest
import random
from ignis_test.executor.core.storage.IPartitionTest import IPartitionTest
from ignis.executor.core.storage.IMemoryPartition import IMemoryPartition
from ignis.executor.core.io.INumpy import INumpyWrapper, disable, enable

disable()


class IMemoryPartitionNumpyTest(IPartitionTest, unittest.TestCase):

	def create(self):
		class INumpyWrapperCl(INumpyWrapper):
			def __init__(self):
				INumpyWrapper.__init__(self, 50, int)
				self.__class__ = INumpyWrapper

		return IMemoryPartition(native=False, cls=INumpyWrapperCl)

	def elemens(self, n):
		random.seed(0)
		return [random.randint(0, n) for i in range(0, n)]

	def setUp(self):
		enable()

	def tearDown(self):
		disable()


class IMemoryPartitionNumpyNativeTest(IPartitionTest, unittest.TestCase):

	def create(self):
		class INumpyWrapperCl(INumpyWrapper):
			def __init__(self):
				INumpyWrapper.__init__(self, 50, int)
				self.__class__ = INumpyWrapper

		return IMemoryPartition(native=True, cls=INumpyWrapperCl)

	def elemens(self, n):
		random.seed(0)
		return [random.randint(0, n) for i in range(0, n)]

	def setUp(self):
		enable()

	def tearDown(self):
		disable()
