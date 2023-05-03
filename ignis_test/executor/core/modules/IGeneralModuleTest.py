import unittest

from ignis.executor.core.io import INumpy
from ignis.executor.core.modules.IGeneralModule import IGeneralModule
from ignis_test.executor.core.IElements import IElementsInt, IElementsStr, IElementsBytes, IElementsPair
from ignis_test.executor.core.modules.IModuleTest import IModuleTest


class IGeneralModuleTest(IModuleTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		IModuleTest.__init__(self)
		unittest.TestCase.__init__(self, *args, **kwargs)
		self.__general = IGeneralModule(self._executor_data)
		props = self._executor_data.getContext().props()
		props["ignis.modules.sort.samples"] = "0.1"
		props["ignis.modules.sort.resampling"] = "False"

	def test_executeToInt(self):
		self.__executeToTest("IntSequence", "RawMemory")

	def test_mapInt(self):
		self.__mapTest("MapInt", "Memory", IElementsInt)

	def test_filterInt(self):
		self.__filterTest("FilterInt", "RawMemory", IElementsInt)

	def test_flatmapString(self):
		self.__flatmapTest("FlatmapString", "Memory", IElementsStr)

	def test_keyByStringInt(self):
		self.__keyByTest("KeyByString", "RawMemory", IElementsStr)

	def test_mapWithIndexInt(self):
		self.__mapWithIndexTest("MapWithIndexInt", "Memory", IElementsInt)

	def test_mapPartitionsInt(self):
		self.__mapPartitionsTest("MapPartitionsInt", "Memory", IElementsInt)

	def test_mapPartitionWithIndexInt(self):
		self.__mapPartitionsWithIndexTest("MapPartitionWithIndexInt", "RawMemory", IElementsInt)

	def test_mapExecutorInt(self):
		self.__mapExecutorTest("MapExecutorInt", "RawMemory", IElementsInt)

	def test_mapExecutorToString(self):
		self.__mapExecutorToTest("MapExecutorToString", "RawMemory", IElementsInt)

	def test_groupByIntString(self):
		self.__groupByTest("GroupByIntString", "Memory", IElementsStr)

	def test_sortBasicInt(self):
		self.__sortTest(None, "Memory", IElementsInt)

	def test_sortInt(self):
		self.__sortTest("SortInt", "RawMemory", IElementsInt)

	def test_sortIntBytes(self):
		self._executor_data.getContext().vars()['STORAGE_CLASS'] = bytearray
		self.__sortTest("SortInt", "Memory", IElementsBytes)

	def test_resamplingSortInt(self):
		self.__sortTest("SortInt", "Memory", IElementsInt, True)

	def test_sortIntNumpy(self):
		INumpy.enable()
		import numpy
		self._executor_data.getContext().vars()['STORAGE_CLASS'] = numpy.ndarray
		self._executor_data.getContext().vars()['STORAGE_CLASS_DTYPE'] = numpy.int64
		self.__sortTest("SortInt", "Memory", IElementsBytes)
		INumpy.disable()

	def test_sortString(self):
		self.__sortTest("SortString", "RawMemory", IElementsStr)

	def test_distinctInt(self):
		self.__distinctTest("Memory", IElementsInt)

	def test_joinStringInt(self):
		self.__joinTest("RawMemory", (IElementsStr, IElementsInt))

	def test_unionInt(self):
		self.__unionTest("Memory", IElementsInt, True)

	def test_unionUnorderedString(self):
		self.__unionTest("RawMemory", IElementsStr, False)

	def test_flatMapValuesInt(self):
		self.__flatMapValuesTest("FlatMapValuesInt", "Memory", (IElementsStr, IElementsInt))

	def test_mapValuesInt(self):
		self.__mapValuesTest("MapValuesInt", "RawMemory", (IElementsStr, IElementsInt))

	def test_groupByKeyIntString(self):
		self.__groupByKeyTest("Memory", (IElementsInt, IElementsStr))

	def test_reduceByKeyIntString(self):
		self.__reduceByKeyTest("ReduceString", "RawMemory", (IElementsInt, IElementsStr))

	def test_aggregateByKeyIntInt(self):
		self.__aggregateByKeyTest("ZeroString", "ReduceIntToString", "ReduceString", "Memory",
		                          (IElementsInt, IElementsInt))

	def test_foldByKeyIntInt(self):
		self.__foldByKeyTest("ZeroInt", "ReduceInt", "Memory", (IElementsInt, IElementsInt))

	def test_sortByKeyIntString(self):
		self.__sortByKeyTest("Memory", (IElementsInt, IElementsStr))

	def test_repartitionOrdered(self):
		self.__repartitionTest("Memory", True, True, IElementsInt)

	def test_repartitionUnordered(self):
		self.__repartitionTest("RawMemory", False, True, IElementsStr)

	def test_repartitionLocal(self):
		self.__repartitionTest("Memory", False, False, IElementsInt)

	def test_partitionBy(self):
		self.__partitionByTest("PartitionByStr", "RawMemory", IElementsStr)

	def test_partitionByRandom(self):
		self.__partitionByRandomTest("Memory", IElementsInt)

	def test_partitionByHash(self):
		self.__partitionByHashTest("RawMemory", IElementsStr)

	# -------------------------------------Impl-------------------------------------

	def __executeToTest(self, name, partitionType):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		self.__general.executeTo(self.newSource(name))
		result = self.getFromPartitions()
		elems = list(range(100))

		self.assertEqual(elems, result)

	def __mapTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.map_(self.newSource(name))
		result = self.getFromPartitions()

		self.assertEqual(list(map(str, elems)), result)

	def __filterTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.filter(self.newSource(name))
		result = self.getFromPartitions()

		j = 0
		for i in range(len(elems)):
			if elems[i] % 2 == 0:
				self.assertEqual(elems[i], result[j])
				j += 1

	def __flatmapTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.flatmap(self.newSource(name))
		result = self.getFromPartitions()

		for i in range(len(elems)):
			self.assertEqual(elems[i], result[2 * i])
			self.assertEqual(elems[i], result[2 * i + 1])

	def __keyByTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.keyBy(self.newSource(name))
		result = self.getFromPartitions()

		for i in range(len(elems)):
			self.assertEqual(len(elems[i]), result[i][0])
			self.assertEqual(elems[i], result[i][1])

	def __mapWithIndexTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.mapWithIndex(self.newSource(name))
		result = self.getFromPartitions()
		offset = self._executor_data.mpi().rank() * len(elems)
		expected = list()
		for i, elem in enumerate(elems):
			expected.append(i + elem + offset)

		self.assertEqual(expected, result)

	def __mapPartitionsTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.mapPartitions(self.newSource(name))
		result = self.getFromPartitions()

		self.assertEqual(list(map(str, elems)), result)

	def __mapPartitionsWithIndexTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.mapPartitionsWithIndex(self.newSource(name))
		result = self.getFromPartitions()

		self.assertEqual(list(map(str, elems)), result)

	def __mapExecutorTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 5)
		self.__general.mapExecutor(self.newSource(name))
		result = self.getFromPartitions()

		for i in range(len(elems)):
			self.assertEqual(elems[i] + 1, result[i])

	def __mapExecutorToTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 5)
		self.__general.mapExecutorTo(self.newSource(name))
		result = self.getFromPartitions()

		self.assertEqual(list(map(str, elems)), result)

	def __groupByTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		elems = IElements().create(100 * 2 * np, 0)
		local_elems = self.rankVector(elems)
		self.loadToPartitions(local_elems, 2)
		self.__general.groupBy(self.newSource(name), 2)
		result = self.getFromPartitions()

		counts = dict()
		for elem in elems:
			if len(elem) in counts:
				counts[len(elem)] += 1
			else:
				counts[len(elem)] = 1

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			for item in result:
				self.assertEqual(counts[item[0]], len(item[1]))

	def __sortTest(self, name, partitionType, IElements, rs=False):
		cores = str(self._executor_data.getContext().executors())
		self._executor_data.getContext().props()["ignis.executor.cores"] = cores
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		self._executor_data.getContext().props()["ignis.modules.sort.resampling"] = "True" if rs else "False"
		np = self._executor_data.getContext().executors()
		elems = IElements().create(100 * 4 * np, 0)
		local_elems = self.rankVector(elems)
		self.loadToPartitions(local_elems, 4)
		if name is None:
			self.__general.sort(True)
		else:
			self.__general.sortBy(self.newSource(name), True)
		result = self.getFromPartitions()

		for i in range(1, len(result)):
			self.assertGreaterEqual(result[i], result[i - 1])

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)

		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			elems.sort()
			for i in range(0, len(result)):
				self.assertEqual(result[i], elems[i])

	def __distinctTest(self, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElements().create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.distinct(4)
		result = self.getFromPartitions()
		distinct = set(elems)

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			self.assertEqual(len(result), len(distinct))
			self.assertEqual(set(result), distinct)

	def __joinTest(self, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		elems = IElementsPair(IElements).create(50 * 2 * np, 0)
		elems2 = IElementsPair(IElements).create(50 * 2 * np, 1)
		local_elems = self.rankVector(elems)
		local_elems2 = self.rankVector(elems2)

		self.loadToPartitions(local_elems2, 2)
		self._executor_data.setVariable("other", self._executor_data.getPartitions())
		self.loadToPartitions(local_elems, 2)

		self.__general.join("other", 2)
		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			m1 = dict()
			for key, value in elems:
				if key not in m1:
					m1[key] = list()
				m1[key].append(value)

			expected = list()
			for key, value2 in elems2:
				if key in m1:
					for value1 in m1[key]:
						expected.append((key, (value1, value2)))

			result.sort()
			expected.sort()

			self.assertEqual(result, expected)

	def __unionTest(self, partitionType, IElements, preserveOrder):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		elems = IElements().create(50 * 2 * np, 0)
		elems2 = IElements().create(50 * 2 * np, 1)
		local_elems = self.rankVector(elems)
		local_elems2 = self.rankVector(elems2)

		self.loadToPartitions(local_elems2, 2)
		self._executor_data.setVariable("other", self._executor_data.getPartitions())
		self.loadToPartitions(local_elems, 2)

		self.__general.union_("other", preserveOrder)
		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			self.assertEqual(len(result), len(elems) + len(elems2))

			if preserveOrder:
				self.assertEqual(result, elems + elems2)
			else:
				expected = elems + elems2
				expected.sort()
				result.sort()
				self.assertEqual(result, expected)

	def __flatMapValuesTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElementsPair(IElements).create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.flatMapValues(self.newSource(name))
		result = self.getFromPartitions()

		for i in range(len(result)):
			self.assertEqual(elems[i][0], result[i][0])
			self.assertEqual(str(elems[i][1]), result[i][1])

	def __mapValuesTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		elems = IElementsPair(IElements).create(100 * 2, 0)
		self.loadToPartitions(elems, 2)
		self.__general.mapValues(self.newSource(name))
		result = self.getFromPartitions()

		for i in range(len(result)):
			self.assertEqual(elems[i][0], result[i][0])
			self.assertEqual(str(elems[i][1]), result[i][1])

	def __groupByKeyTest(self, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		elems = IElementsPair(IElements).create(100 * 2 * np, 0)
		local_elems = self.rankVector(elems)
		self.loadToPartitions(local_elems, 2)
		self.__general.groupByKey(2)
		result = self.getFromPartitions()

		counts = dict()
		for key, value in elems:
			if key in counts:
				counts[key] += 1
			else:
				counts[key] = 1

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			for item in result:
				self.assertEqual(counts[item[0]], len(item[1]))

	def __reduceByKeyTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		elems = IElementsPair(IElements).create(100 * 2 * np, 0)
		local_elems = self.rankVector(elems)
		self.loadToPartitions(local_elems, 2)
		self.__general.reduceByKey(self.newSource(name), 2, True)
		result = self.getFromPartitions()

		counts = dict()
		for key, value in elems:
			if key in counts:
				counts[key] += value
			else:
				counts[key] = value

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			for item in result:
				self.assertEqual(self._normalize(counts[item[0]]), self._normalize(item[1]))

	def __aggregateByKeyTest(self, zero, seq, comb, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		elems = IElementsPair(IElements).create(100 * 2 * np, 0)
		local_elems = self.rankVector(elems)
		self.loadToPartitions(local_elems, 2)
		self.__general.aggregateByKey4(self.newSource(zero), self.newSource(seq), self.newSource(comb), 2)
		result = self.getFromPartitions()

		counts = dict()
		for key, value in elems:
			if key in counts:
				counts[key] += str(value)
			else:
				counts[key] = str(value)

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			for item in result:
				self.assertEqual(self._normalize(counts[item[0]]), self._normalize(item[1]))

	def __foldByKeyTest(self, zero, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		elems = IElementsPair(IElements).create(100 * 2 * np, 0)
		local_elems = self.rankVector(elems)
		self.loadToPartitions(local_elems, 2)
		self.__general.foldByKey(self.newSource(zero), self.newSource(name), 2, True)
		result = self.getFromPartitions()

		counts = dict()
		for key, value in elems:
			if key in counts:
				counts[key] += value
			else:
				counts[key] = value

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			for item in result:
				self.assertEqual(self._normalize(counts[item[0]]), self._normalize(item[1]))

	def __sortByKeyTest(self, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		elems = IElementsPair(IElements).create(100 * 4 * np, 0)
		local_elems = self.rankVector(elems)
		self.loadToPartitions(local_elems, 4)
		self.__general.sortByKey(True)
		result = self.getFromPartitions()

		for i in range(1, len(result)):
			self.assertGreaterEqual(result[i], result[i - 1])

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)

		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			elems.sort()
			for i in range(1, len(result)):
				self.assertEqual(result[i][0], elems[i][0])

	def __repartitionTest(self, partitionType, preserveOrdering, global_, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		rank = self._executor_data.mpi().rank()
		elems = IElements().create(100 * 2 * np, 0)
		local_elems = (elems[0:100] if rank == 0 else []) + self.rankVector(elems)
		elems = elems[0:100] + elems
		self.loadToPartitions(local_elems, 2)

		self.__general.repartition(2 * np - 1, preserveOrdering, global_)

		for part in self._executor_data.getPartitions():
			self.assertGreater(len(part), 50)

		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			self.assertEqual(len(result), len(elems))
			if preserveOrdering or not global_:
				self.assertEqual(result, elems)
			else:
				expected = dict()
				for e in elems:
					expected[e] = expected.get(e, 0) + 1
				for e in result:
					expected[e] = expected.get(e, 0) - 1
				self.assertEqual(min(expected.values()), 0)
				self.assertEqual(max(expected.values()), 0)

	def __partitionByTest(self, name, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		rank = self._executor_data.mpi().rank()
		elems = IElements().create(100 * 2 * np, 0)
		local_elems = (elems[0:100] if rank == 0 else []) + self.rankVector(elems)
		elems = elems[0:100] + elems
		self.loadToPartitions(local_elems, 2)

		self.__general.partitionBy(self.newSource(name),2 * np - 1)

		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			expected = dict()
			for e in elems:
				expected[e] = expected.get(e, 0) + 1
			for e in result:
				expected[e] = expected.get(e, 0) - 1
			self.assertEqual(min(expected.values()), 0)
			self.assertEqual(max(expected.values()), 0)

	def __partitionByRandomTest(self, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		rank = self._executor_data.mpi().rank()
		elems = IElements().create(100 * 2 * np, 0)
		local_elems = (elems[0:100] if rank == 0 else []) + self.rankVector(elems)
		elems = elems[0:100] + elems
		self.loadToPartitions(local_elems, 2)

		self.__general.partitionByRandom(2 * np - 1, 0)

		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			expected = dict()
			for e in elems:
				expected[e] = expected.get(e, 0) + 1
			for e in result:
				expected[e] = expected.get(e, 0) - 1
			self.assertEqual(min(expected.values()), 0)
			self.assertEqual(max(expected.values()), 0)

	def __partitionByHashTest(self, partitionType, IElements):
		self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
		np = self._executor_data.getContext().executors()
		rank = self._executor_data.mpi().rank()
		elems = IElements().create(100 * 2 * np, 0)
		local_elems = (elems[0:100] if rank == 0 else []) + self.rankVector(elems)
		elems = elems[0:100] + elems
		self.loadToPartitions(local_elems, 2)

		self.__general.partitionByHash(2 * np - 1)

		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			expected = dict()
			for e in elems:
				expected[e] = expected.get(e, 0) + 1
			for e in result:
				expected[e] = expected.get(e, 0) - 1
			self.assertEqual(min(expected.values()), 0)
			self.assertEqual(max(expected.values()), 0)
