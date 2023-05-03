import unittest
import random
from ignis_test.executor.core.storage.IPartitionTest import IPartitionTest
from ignis.executor.core.storage.IMemoryPartition import IMemoryPartition


class IMemoryPartitionTest(IPartitionTest, unittest.TestCase):

	def create(self):
		return IMemoryPartition(native=False)

	def elemens(self, n):
		random.seed(0)
		return [random.randint(0, n) for i in range(0, n)]


class IMemoryPartitionNativeTest(IPartitionTest, unittest.TestCase):

	def create(self):
		return IMemoryPartition(native=True)

	def elemens(self, n):
		random.seed(0)
		return [random.randint(0, n) for i in range(0, n)]
