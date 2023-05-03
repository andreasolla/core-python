from ignis.executor.api.function.IFunction import IFunction
from ignis.executor.api.function.IFunction0 import IFunction0
from ignis.executor.api.function.IFunction2 import IFunction2
from ignis.executor.api.function.IVoidFunction import IVoidFunction
from ignis.executor.api.function.IVoidFunction0 import IVoidFunction0


class IntSequence(IFunction0):

    def call(self, context):
        return [list(range(100))]

class NoneFunction(IVoidFunction0):

    def call(self, context):
        context.vars()["test"] = True

class MapInt(IFunction):

    def call(self, v, context):
        return str(v)


class FilterInt(IFunction):

    def call(self, v, context):
        return v % 2 == 0


class FlatmapString(IFunction):

    def call(self, v, context):
        return [v, v]


class KeyByString(IFunction):

    def call(self, v, context):
        return len(v)


class MapWithIndexInt(IFunction2):

    def call(self, v1, v2, context):
        return v1 + v2

class MapPartitionsInt(IFunction):

    def call(self, it, context):
        v = list()
        for i in it:
            v.append(str(i))
        return v


class MapPartitionWithIndexInt(IFunction2):
    def call(self, p, it, context):
        v = list()
        for i in it:
            v.append(str(i))
        return v


class MapExecutorInt(IVoidFunction):
    def call(self, parts, context):
        for part in parts:
            for i in range(len(part)):
                part[i] += 1


class MapExecutorToString(IFunction):
    def call(self, parts, context):
        v = list()
        for part in parts:
            v.append([str(item) for item in part])
        return v


class GroupByIntString(IFunction):
    def call(self, elem, context):
        return len(elem)


class ReduceInt(IFunction2):
    def call(self, v1, v2, context):
        return v1 + v2


class ReduceString(IFunction2):
    def call(self, v1, v2, context):
        return v1 + v2


class SortInt(IFunction2):
    def call(self, v1, v2, context):
        return v1 < v2


class SortString(IFunction2):
    def call(self, v1, v2, context):
        return v1 < v2


class MapValuesInt(IFunction):
    def call(self, v, context):
        return str(v)


class FlatMapValuesInt(IFunction):
    def call(self, v, context):
        return [str(v)]


class ZeroInt(IFunction0):
    def call(self, context):
        return 0


class ReduceIntToString(IFunction2):
    def call(self, v1, v2, context):
        return v1 + str(v2)


class ZeroString(IFunction0):
    def call(self, context):
        return ""


class ForeachInt(IVoidFunction):
    def call(self, v, context):
        context.vars()["test"] = True


class ForeachPartitionString(IVoidFunction):
    def call(self, it, context):
        for i in it:
            pass
        context.vars()["test"] = True


class ForeachExecutorString(IVoidFunction):
    def call(self, parts, context):
        context.vars()["test"] = True


class PartitionByStr(IFunction):

    def call(self, e, context):
        return hash(e)