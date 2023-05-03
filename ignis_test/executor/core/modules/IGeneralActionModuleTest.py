import unittest

from ignis.executor.core.modules.IGeneralActionModule import IGeneralActionModule
from ignis.rpc.source.ttypes import IEncoded, ISource
from ignis_test.executor.core.IElements import IElementsInt, IElementsStr, IElementsPair
from ignis_test.executor.core.modules.IModuleTest import IModuleTest


class IGeneralActionModuleTest(IModuleTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        IModuleTest.__init__(self)
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.__generalAction = IGeneralActionModule(self._executor_data)

    def test_executeNone(self):
        self.__executeTest("NoneFunction")

    def test_loadAndRefNone(self):
        self.__loadAndRefTest("NoneFunction")

    def test_reduceInt(self):
        self.__reduceTest("ReduceInt", "Memory", IElementsInt)

    def test_reduceString(self):
        self.__reduceTest("ReduceString", "RawMemory", IElementsStr)

    def test_treeReduceInt(self):
        self.__treeReduceTest("ReduceInt", "Memory", IElementsInt)

    def test_treeReduceString(self):
        self.__treeReduceTest("ReduceString", "RawMemory", IElementsStr)

    def test_aggregateIntToString(self):
        self.__aggregateTest("ZeroString", "ReduceIntToString", "ReduceString", "Memory", IElementsInt)

    def test_treeAggregateString(self):
        self.__treeAggregateTest("ZeroString", "ReduceString", "ReduceString", "Memory", IElementsStr)

    def test_foldIntTest(self):
        self.__foldTest("ZeroInt", "ReduceInt", "Memory", IElementsInt)

    def test_treeFoldStringTest(self):
        self.__treeFoldTest("ZeroString", "ReduceString", "Memory", IElementsStr)

    def test_takeStringTest(self):
        self.__takeTest("Memory", IElementsStr)

    def test_foreachInt(self):
        self.__foreachTest("ForeachInt", "Memory", IElementsInt)

    def test_foreachPartitionString(self):
        self.__foreachPartitionTest("ForeachPartitionString", "Memory", IElementsStr)

    def test_foreachExecutorString(self):
        self.__foreachExecutorTest("ForeachExecutorString", "RawMemory", IElementsStr)

    def test_topInt(self):
        self.__topTest("Memory", IElementsInt)

    def test_customTopString(self):
        self.__customTopTest("SortString", "RawMemory", IElementsStr)

    def test_takeOrderedString(self):
        self.__takeOrderedTest("RawMemory", IElementsStr)

    def test_customTakeOrderedInt(self):
        self.__customTakeOrderedTest("SortInt", "Memory", IElementsInt)

    def test_keysIntString(self):
        self.__keysTest("Memory", (IElementsInt, IElementsStr))

    def test_valuesStringInt(self):
        self.__valuesTest("RawMemory", (IElementsStr, IElementsInt))

    # -------------------------------------Impl-------------------------------------

    def __executeTest(self, name):
        self.__generalAction.execute(self.newSource(name))
        self.assertTrue(self._executor_data.getContext().vars()["test"])

    def __loadAndRefTest(self, name):
        self.__generalAction._use_source(self.newSource(name))
        src = ISource(obj=IEncoded(name=name))
        self.__generalAction.execute(src)

    def __reduceTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElements().create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__generalAction.reduce(self.newSource(name))
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(1, len(result))
            expected_result = elems[0]
            for i in range(1, len(elems)):
                expected_result += elems[i]
            self.assertEqual(expected_result, result[0])
        else:
            self.assertEqual(0, len(result))

    def __treeReduceTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElements().create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__generalAction.treeReduce(self.newSource(name))
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(1, len(result))
            expected_result = elems[0]
            for i in range(1, len(elems)):
                expected_result += elems[i]
            self.assertEqual(expected_result, result[0])
        else:
            self.assertEqual(0, len(result))

    def __aggregateTest(self, zero, seq, comb, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElements().create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__generalAction.aggregate(self.newSource(zero), self.newSource(seq), self.newSource(comb))
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(1, len(result))
            expected_result = str(elems[0])
            for i in range(1, len(elems)):
                expected_result += str(elems[i])
            self.assertEqual(expected_result, result[0])
        else:
            self.assertEqual(0, len(result))

    def __treeAggregateTest(self, zero, seq, comb, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElements().create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__generalAction.treeAggregate(self.newSource(zero), self.newSource(seq), self.newSource(comb))
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(1, len(result))
            expected_result = elems[0]
            for i in range(1, len(elems)):
                expected_result += elems[i]
            expected_result = self._normalize(expected_result)
            result[0] = self._normalize(result[0])
            self.assertEqual(expected_result, result[0])
        else:
            self.assertEqual(0, len(result))

    def __foldTest(self, zero, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElements().create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__generalAction.fold(self.newSource(zero), self.newSource(name))
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(1, len(result))
            expected_result = elems[0]
            for i in range(1, len(elems)):
                expected_result += elems[i]
            self.assertEqual(expected_result, result[0])
        else:
            self.assertEqual(0, len(result))

    def __treeFoldTest(self, zero, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElements().create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__generalAction.treeFold(self.newSource(zero), self.newSource(name))
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(1, len(result))
            expected_result = elems[0]
            for i in range(1, len(elems)):
                expected_result += elems[i]
            expected_result = self._normalize(expected_result)
            result[0] = self._normalize(result[0])
            self.assertEqual(expected_result, result[0])
        else:
            self.assertEqual(0, len(result))

    def __takeTest(self, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        n = 30
        elems = IElements().create(100, 0)
        self.loadToPartitions(elems, 5)
        self.__generalAction.take(n)
        result = self.getFromPartitions()

        self.assertEqual(n, len(result))
        self.assertEqual(elems[0:n], result)

    def __foreachTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100, 0)
        self.loadToPartitions(elems, 2)
        self.__generalAction.foreach_(self.newSource(name))
        self.assertTrue(self._executor_data.getContext().vars()["test"])

    def __foreachPartitionTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100, 0)
        self.loadToPartitions(elems, 2)
        self.__generalAction.foreachPartition(self.newSource(name))
        self.assertTrue(self._executor_data.getContext().vars()["test"])

    def __foreachExecutorTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100, 0)
        self.loadToPartitions(elems, 2)
        self.__generalAction.foreachExecutor(self.newSource(name))
        self.assertTrue(self._executor_data.getContext().vars()["test"])

    def __topTest(self, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        n = 30
        elems = IElements().create(100 * 5 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 5)
        self.__generalAction.top(n)
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(n, len(result))
            elems.sort(reverse=True)
            self.assertEqual(elems[0:n], result)

    def __customTopTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        n = 30
        elems = IElements().create(100 * 5 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 5)
        self.__generalAction.top2(n, self.newSource(name))
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(n, len(result))
            elems.sort(reverse=True)
            self.assertEqual(elems[0:n], result)

    def __takeOrderedTest(self, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        n = 30
        elems = IElements().create(100 * 5 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 5)
        self.__generalAction.takeOrdered(n)
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(n, len(result))
            elems.sort(reverse=False)
            self.assertEqual(elems[0:n], result)

    def __customTakeOrderedTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        n = 30
        elems = IElements().create(100 * 5 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 5)
        self.__generalAction.takeOrdered2(n, self.newSource(name))
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            self.assertEqual(n, len(result))
            elems.sort(reverse=False)
            self.assertEqual(elems[0:n], result)

    def __keysTest(self, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElementsPair(IElements).create(100, 0)
        self.loadToPartitions(elems, 2)
        self.__generalAction.keys()
        result = self.getFromPartitions()

        self.assertEqual([item[0] for item in elems], result)

    def __valuesTest(self, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElementsPair(IElements).create(100, 0)
        self.loadToPartitions(elems, 2)
        self.__generalAction.values()
        result = self.getFromPartitions()

        self.assertEqual([item[1] for item in elems], result)
