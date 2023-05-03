import math
import unittest

from ignis.executor.core.modules.ICommModule import ICommModule
from ignis_test.executor.core.IElements import IElementsInt, IElementsStr
from ignis_test.executor.core.modules.IModuleTest import IModuleTest


class ICommModuleTest(IModuleTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		IModuleTest.__init__(self)
		unittest.TestCase.__init__(self, *args, **kwargs)
		self.__comm = ICommModule(self._executor_data)
		props = self._executor_data.getContext().props()

	def test_getSetPartitionsEmpty(self):
		self.__getSetPartitions("RawMemory", 0, 4, IElementsInt)

	def test_getSetPartitionsSimple(self):
		self.__getSetPartitions("Memory", 1, 4, IElementsInt)

	def test_getSetPartitionsHasMinimal(self):
		self.__getSetPartitions("RawMemory", 4, 2, IElementsStr)

	def test_getSetPartitionsAdvanced(self):
		self.__getSetPartitions("Memory", 2, 4, IElementsInt)

	def test_importData(self):
		self.__importDataTest("Memory", 10, IElementsInt)

	# -------------------------------------Impl-------------------------------------

	def __getSetPartitions(self, partitionType, parts, getParts, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * max(parts, 1), 0)
		self.loadToPartitions(elems, max(parts, 1))
		if parts == 0:
			elems = []
			self._executor_data.getPartitions().clear()
		binary = self.__comm.getPartitions2(self.__comm.getProtocol(), getParts)
		self.__comm.setPartitions(binary)
		result = self.getFromPartitions()
		self.assertEqual(result, elems)

	def __importDataTest(self, partitionType, parts, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		bak = self._executor_data.mpi().native()
		executors = self._executor_data.mpi().executors()
		rank = self._executor_data.mpi().rank()
		sources = math.ceil(0.25 * executors)
		color = 1 if rank < sources else 0
		group = bak.Split(color, rank)
		self._executor_data.setMpiGroup(group)
		elems = IElements().create(200 * parts * (executors - sources), 0)
		address = ""
		if color == 0:
			local_elems = self.rankVector(elems)
			self.loadToPartitions(local_elems, parts)
			if self._executor_data.mpi().isRoot(0):
				address = self.__comm.openGroup()
				with open("../address.txt", "w") as file:
					file.write(address)
					file.flush()
			bak.Barrier()
		else:
			bak.Barrier()
			with open("../address.txt") as file:
				address = file.readline()

		self.__comm.joinToGroupName(address, color == 0, "group")
		self.__comm.closeGroup()

		self.__comm.importData("group", color == 0, 1)

		if color == 1:
			result = self.getFromPartitions()
			self.loadToPartitions(result, 1)
			self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
			result = self.getFromPartitions()
			if self._executor_data.mpi().isRoot(0):
				self.assertEqual(result, elems)

		self.__comm.destroyGroup("group")
		group.Free()
		self._executor_data.setMpiGroup(bak)
		self._executor_data.mpi().barrier()

