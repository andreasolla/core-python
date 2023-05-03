import glob
import math
import os
import shutil
import unittest

from ignis.executor.core.modules.IIOModule import IIOModule
from ignis_test.executor.core.IElements import IElementsStr
from ignis_test.executor.core.modules.IModuleTest import IModuleTest


class IIOModuleTest(IModuleTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		IModuleTest.__init__(self)
		unittest.TestCase.__init__(self, *args, **kwargs)
		self.__io = IIOModule(self._executor_data)
		props = self._executor_data.getContext().props()
		props["ignis.partition.minimal"] = "10MB"
		props["ignis.partition.type"] = "Memory"
		props["ignis.modules.io.overwrite"] = "true"

	def test_textFile1(self):
		self.__textFileTest(1)

	def test_textFileN(self):
		self.__textFileTest(8)

	def test_plainFile1(self):
		self.__plainFileTest(1, "@")

	def test_plainFileN(self):
		self.__plainFileTest(8, "@")

	def test_plainFileS1(self):
		self.__plainFileTest(1, "@@")

	def test_plainFileSN(self):
		self.__plainFileTest(8, "@@")

	def test_plainFileSE1(self):
		self.__plainFileTest(1, "@@", "!")

	def test_plainFileSEN(self):
		self.__plainFileTest(8, "@@", "!")

	def test_saveAsTextFile(self):
		self.__saveAsTextFileTest(8)

	def test_partitionTextFile(self):
		self.__partitionTextFileTest()

	def test_partitionJsonFile(self):
		self.__partitionJsonFileTest()

	def test_partitionObjectFile(self):
		self.__partitionObjectFileTest()

	# -------------------------------------Impl-------------------------------------

	def __plainFileTest(self, n, delim, ex=""):
		path = "./testplainfile.txt"
		np = self._executor_data.mpi().executors()
		lines = list()
		with open(path, "w") as file:
			for i in range(100):
				line = ''.join(IElementsStr().create(100, i))
				if ex:
					line += ex + delim
				file.write(line)
				file.write(delim)
				lines.append(line)

		if len(ex) > 0:
			ex = ex.replace("!", "\\!")
			delim += "!" + ex

		self.__io.plainFile3(path, n, delim)

		self.assertGreaterEqual(math.ceil(n / np), self.__io.partitionCount())

		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			self.assertEqual(lines, result)

	def __textFileTest(self, n):
		path = "./testfile.txt"
		np = self._executor_data.mpi().executors()
		lines = list()
		with open(path, "w") as file:
			for i in range(100):
				line = ''.join(IElementsStr().create(100, i))
				file.write(line)
				file.write('\n')
				lines.append(line)

		self.__io.textFile2(path, n)

		self.assertGreaterEqual(math.ceil(n / np), self.__io.partitionCount())

		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			self.assertEqual(lines, result)

	def __saveAsTextFileTest(self, n):
		path = "./savetest.txt"
		rank = self._executor_data.mpi().rank()
		np = self._executor_data.mpi().executors()
		if np > 0:
			path = "." + path
		lines = list()
		for i in range(20 * n * np):
			line = ''.join(IElementsStr().create(10, i))
			lines.append(line)

		local_lines = self.rankVector(lines)
		self.loadToPartitions(local_lines, n)
		if self._executor_data.mpi().isRoot(0) and os.path.exists(path):
			shutil.rmtree(path)
		self._executor_data.mpi().barrier()
		self.__io.saveAsTextFile(path, rank * n)
		self._executor_data.mpi().barrier()
		if self._executor_data.mpi().isRoot(0):
			parts = sorted(glob.glob(path + "/*"))
			self.assertEqual(len(parts), n * np)
			result = list()
			for part in parts:
				with open(part, "r") as file:
					for line in file:
						result.append(line.strip())
			self.assertEqual(result, lines)

	def __partitionTextFileTest(self):
		path = "./savetest.txt"
		n = 8
		rank = self._executor_data.mpi().rank()
		lines = list()
		for i in range(100 * n):
			line = ''.join(IElementsStr().create(10, i))
			lines.append(line)

		self.loadToPartitions(lines, n)
		if os.path.exists(path):
			shutil.rmtree(path)
		self.__io.saveAsTextFile(path, rank * n)
		self.__io.partitionTextFile(path, rank * n, n)

		result = self.getFromPartitions()
		self.assertEqual(result, lines)

	def __partitionJsonFileTest(self):
		path = "./savetestjson"
		n = 8
		rank = self._executor_data.mpi().rank()
		lines = list()
		for i in range(100 * n):
			line = ''.join(IElementsStr().create(10, i))
			lines.append(line)

		self.loadToPartitions(lines, n)
		if os.path.exists(path):
			shutil.rmtree(path)
		self.__io.saveAsJsonFile(path, rank * n, True)
		self.__io.partitionJsonFile4a(path, rank * n, n, True)

		result = self.getFromPartitions()
		self.assertEqual(result, lines)

	def __partitionObjectFileTest(self):
		path = "./savetestbin"
		n = 8
		rank = self._executor_data.mpi().rank()
		lines = list()
		for i in range(100 * n):
			line = ''.join(IElementsStr().create(10, i))
			lines.append(line)

		self.loadToPartitions(lines, n)
		if os.path.exists(path):
			shutil.rmtree(path)
		self.__io.saveAsObjectFile(path, 0, rank * n)
		self.__io.partitionObjectFile(path, rank * n, n)

		result = self.getFromPartitions()
		self.assertEqual(result, lines)
