import unittest
import random
from ignis_test.executor.core.storage.IPartitionTest import IPartitionTest
from ignis.executor.core.storage.IMemoryPartition import IMemoryPartition


class IMemoryPartitionBytearrayTest(IPartitionTest, unittest.TestCase):

	def create(self):
		return IMemoryPartition(native=False, cls=bytearray)

	def elemens(self, n):
		random.seed(0)
		return bytearray([random.randint(0, n) for i in range(0, n % 256)])

	def _readIterator(self, part):
		elems = bytearray()
		it = part.readIterator()
		while it.hasNext():
			elems.append(it.next())
		return elems

	def _write(self, part, native):
		return bytearray(super()._write(part,native))


class IMemoryPartitionBytearrayNativeTest(IPartitionTest, unittest.TestCase):

	def create(self):
		return IMemoryPartition(native=True, cls=bytearray)

	def elemens(self, n):
		random.seed(0)
		return bytearray([random.randint(0, n) for i in range(0, n % 256)])

	def _readIterator(self, part):
		elems = bytearray()
		it = part.readIterator()
		while it.hasNext():
			elems.append(it.next())
		return elems

	def _write(self, part, native):
		return bytearray(super()._write(part,native))
