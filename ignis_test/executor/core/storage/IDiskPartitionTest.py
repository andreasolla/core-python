import unittest
import random
from abc import ABC
import os

from ignis_test.executor.core.storage.IPartitionTest import IPartitionTest
from ignis.executor.core.storage.IDiskPartition import IDiskPartition


class IDiskPartitionTestAbs(IPartitionTest, ABC):
	_id = 0

	def test_rename(self):
		path = "./diskpartitionTest"
		newPath = "./diskpartitionTestRename"
		part = IDiskPartition(native=False, path=path, compression=6)
		elems = self.elemens(100)
		self._writeIterator(elems, part)
		part.rename(newPath)
		self.assert_(not os.path.exists(path))
		self.assertEqual(len(elems), len(part))
		result = self._readIterator(part)
		self.assert_(elems == result)

	def test_persist(self):
		path = "./diskpartitionTest"
		part = IDiskPartition(native=False, path=path, compression=6, persist=True)
		elems = self.elemens(100)
		self._writeIterator(elems, part)
		part.persist(True)
		part.sync()
		del part
		part = IDiskPartition(native=False, path=path, compression=6, persist=True, read=True)
		self.assertEqual(len(elems), len(part))
		result = self._readIterator(part)
		self.assert_(elems == result)


class IDiskPartitionTest(IDiskPartitionTestAbs, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IDiskPartitionTestAbs.__init__(self)

	def create(self):
		IDiskPartitionTestAbs._id += 1
		return IDiskPartition(native=False, path="./diskpartitionTest" + str(IDiskPartitionTestAbs._id), compression=6)

	def elemens(self, n):
		random.seed(0)
		return [random.randint(0, n) for i in range(0, n)]


class IDiskPartitionPairTest(IDiskPartitionTestAbs, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IDiskPartitionTestAbs.__init__(self)

	def create(self):
		IDiskPartitionTestAbs._id += 1
		return IDiskPartition(native=False, path="./diskpartitionTest" + str(IDiskPartitionTestAbs._id), compression=6)

	def elemens(self, n):
		random.seed(0)
		return [(str(random.randint(0, n)), random.randint(0, n)) for i in range(0, n)]


class IDiskPartitionNativeTest(IDiskPartitionTestAbs, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IDiskPartitionTestAbs.__init__(self)

	def create(self):
		IDiskPartitionTestAbs._id += 1
		return IDiskPartition(native=True, path="./diskpartitionTest" + str(IDiskPartitionTestAbs._id), compression=6)

	def elemens(self, n):
		return [random.randint(0, n) for i in range(0, n)]
