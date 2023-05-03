import enum

import ignis.rpc.driver.exception.ttypes
from ignis.driver.api.IDriverException import IDriverException
from ignis.driver.api.ISource import ISource
from ignis.driver.api.Ignis import Ignis


class ICacheLevel(enum.Enum):
    NO_CACHE = 0
    PRESERVE = 1
    MEMORY = 2
    RAW_MEMORY = 3
    DISK = 4


class IDataFrame:

    def __init__(self, _id):
        self._id = _id

    def setName(self, name):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().setName(self._id, name)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def persist(self, cacheLevel):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().persist(self._id, cacheLevel.value)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def cache(self):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().cache(self._id)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def unpersist(self):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().unpersist(self._id)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def uncache(self):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().uncache(self._id)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def partitions(self):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().partitions(self._id))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def saveAsObjectFile(self, path, compression=6):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().saveAsObjectFile(self._id, path, compression)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def saveAsTextFile(self, path):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().saveAsTextFile(self._id, path)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def saveAsJsonFile(self, path, pretty=True):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().saveAsJsonFile(self._id, path, pretty)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def repartition(self, numPartitions, preserveOrdering, global_):
        try:
            with Ignis._clientPool().getClient() as client:
                self._id = client.getDataFrameService().repartition(self._id, numPartitions, preserveOrdering, global_)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def partitionByRandom(self, numPartitions, seed):
        try:
            with Ignis._clientPool().getClient() as client:
                self._id = client.getDataFrameService().partitionByRandom(self._id, numPartitions, seed)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def partitionByHash(self, numPartitions):
        try:
            with Ignis._clientPool().getClient() as client:
                self._id = client.getDataFrameService().partitionByHash(self._id, numPartitions)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def partitionBy(self, src, numPartitions):
        try:
            with Ignis._clientPool().getClient() as client:
                self._id = client.getDataFrameService().partitionBy(self._id, ISource.wrap(src).rpc(),numPartitions)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def map(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().map_(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def filter(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().filter(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def flatmap(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().flatmap(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def keyBy(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IPairDataFrame(client.getDataFrameService().keyBy(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def mapWithIndex(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().mapWithIndex(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def mapPartitions(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().mapPartitions(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def mapPartitionsWithIndex(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().mapPartitionsWithIndex(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def mapExecutor(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().mapExecutor(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def mapExecutorTo(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().mapExecutorTo(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def groupBy(self, src, numPartitions=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if numPartitions is None:
                    return IPairDataFrame(client.getDataFrameService().groupBy(self._id, ISource.wrap(src).rpc()))
                else:
                    return IPairDataFrame(
                        client.getDataFrameService().groupBy2(self._id, ISource.wrap(src).rpc(), numPartitions))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def sort(self, ascending=True, numPartitions=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if numPartitions is None:
                    return IDataFrame(client.getDataFrameService().sort(self._id, ascending))
                else:
                    return IDataFrame(client.getDataFrameService().sort2(self._id, ascending, numPartitions))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def sortBy(self, src, ascending=True, numPartitions=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if numPartitions is None:
                    return IDataFrame(client.getDataFrameService().sortBy(self._id, ISource.wrap(src).rpc(), ascending))
                else:
                    return IDataFrame(client.getDataFrameService().sortBy3(self._id, ISource.wrap(src).rpc(), ascending,
                                                                           numPartitions))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def union(self, other, preserveOrder=False, src=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if src is None:
                    return Ignis._driverContext().collect(
                        client.getDataFrameService().union_(self._id, other._id, preserveOrder)
                    )
                else:
                    return Ignis._driverContext().collect(
                        client.getDataFrameService().union4(self._id, other._id, preserveOrder, ISource.wrap(src).rpc())
                    )

        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def distinct(self, numPartitions=None, src=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if src is None:
                    if numPartitions is None:
                        return IDataFrame(
                            client.getDataFrameService().join(self._id)
                        )
                    else:
                        return IDataFrame(
                            client.getDataFrameService().join2a(self._id, numPartitions)
                        )
                else:
                    if numPartitions is None:
                        return IDataFrame(
                            client.getDataFrameService().join2b(self._id, ISource.wrap(src).rpc())
                        )
                    else:
                        return IDataFrame(
                            client.getDataFrameService().join3(self._id, numPartitions, ISource.wrap(src).rpc())
                        )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def reduce(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect1(
                    client.getDataFrameService().reduce(self._id, ISource.wrap(src).rpc(), ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def treeReduce(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect1(
                    client.getDataFrameService().treeReduce(self._id, ISource.wrap(src).rpc(), ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def collect(self):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect(
                    client.getDataFrameService().collect(self._id, ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def aggregate(self, zero, seqOp, combOp):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect1(
                    client.getDataFrameService().aggregate(self._id,
                                                           ISource.wrap(zero).rpc(),
                                                           ISource.wrap(seqOp).rpc(),
                                                           ISource.wrap(combOp).rpc(),
                                                           ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def treeAggregate(self, zero, seqOp, combOp):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect1(
                    client.getDataFrameService().treeAggregate(self._id,
                                                               ISource.wrap(zero).rpc(),
                                                               ISource.wrap(seqOp).rpc(),
                                                               ISource.wrap(combOp).rpc(),
                                                               ISource("").rpc())
                )

        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def fold(self, zero, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect1(
                    client.getDataFrameService().fold(self._id,
                                                      ISource.wrap(zero).rpc(),
                                                      ISource.wrap(src).rpc(),
                                                      ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def treeFold(self, zero, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect1(
                    client.getDataFrameService().treeFold(self._id,
                                                          ISource.wrap(zero).rpc(),
                                                          ISource.wrap(src).rpc(),
                                                          ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def take(self, num):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect(
                    client.getDataFrameService().take(self._id, num, ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def foreach(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().foreach(self._id, ISource.wrap(src).rpc())
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def foreachPartition(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().foreachPartition(self._id, ISource.wrap(src).rpc())
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def foreachExecutor(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                client.getDataFrameService().foreachExecutor(self._id, ISource.wrap(src).rpc())
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def top(self, num, cmp=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if cmp is None:
                    return Ignis._driverContext().collect(
                        client.getDataFrameService().top(self._id, num, ISource("").rpc())
                    )
                else:
                    return Ignis._driverContext().collect(
                        client.getDataFrameService().top4(self._id, num, ISource.wrap(cmp).rpc(), ISource("").rpc())
                    )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def takeOrdered(self, num, cmp=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if cmp is None:
                    return Ignis._driverContext().collect(
                        client.getDataFrameService().takeOrdered(self._id, num, ISource("").rpc())
                    )
                else:
                    return Ignis._driverContext().collect(
                        client.getDataFrameService().takeOrdered4(self._id, num, ISource.wrap(cmp).rpc(),
                                                                  ISource("").rpc())
                    )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def sample(self, withReplacement, fraction, seed):
        try:
            with Ignis._clientPool().getClient() as client:
                return IDataFrame(client.getDataFrameService().sample(self._id, withReplacement, fraction, seed))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def takeSample(self, withReplacement, num, seed):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect(
                    client.getDataFrameService().takeSample(self._id, withReplacement, num, seed, ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def count(self):
        try:
            with Ignis._clientPool().getClient() as client:
                return client.getDataFrameService().count(self._id)
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def max(self, cmp=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if cmp is None:
                    return Ignis._driverContext().collect1(
                        client.getDataFrameService().max(self._id, ISource("").rpc())
                    )
                else:
                    return Ignis._driverContext().collect1(
                        client.getDataFrameService().max3(self._id, ISource.wrap(cmp).rpc(), ISource("").rpc())
                    )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def min(self, cmp=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if cmp is None:
                    return Ignis._driverContext().collect1(
                        client.getDataFrameService().min(self._id, ISource("").rpc())
                    )
                else:
                    return Ignis._driverContext().collect1(
                        client.getDataFrameService().min3(self._id, ISource.wrap(cmp).rpc(), ISource("").rpc())
                    )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def toPair(self):
        return IPairDataFrame(self._id)

    def __ne__(self, o: object) -> bool:
        return not (self == o)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, IDataFrame) and self._id == o._id


class IPairDataFrame(IDataFrame):

    def __init__(self, _id):
        IDataFrame.__init__(self, _id)

    def toPair(self):
        return self

    def join(self, other, numPartitions=None, src=None):
        try:
            with Ignis._clientPool().getClient() as client:
                if src is None:
                    if numPartitions is None:
                        return IDataFrame(
                            client.getDataFrameService().join(self._id, other._id)
                        )
                    else:
                        return IDataFrame(
                            client.getDataFrameService().join3a(self._id, other._id, numPartitions)
                        )
                else:
                    if numPartitions is None:
                        return IDataFrame(
                            client.getDataFrameService().join3b(self._id, other._id, ISource.wrap(src).rpc())
                        )
                    else:
                        return IDataFrame(
                            client.getDataFrameService().join4(self._id, other._id, numPartitions, ISource.wrap(src).rpc())
                        )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def flatMapValues(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IPairDataFrame(client.getDataFrameService().flatMapValues(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def mapValues(self, src):
        try:
            with Ignis._clientPool().getClient() as client:
                return IPairDataFrame(client.getDataFrameService().mapValues(self._id, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def groupByKey(self, numPartitions=None, src=None):
        try:
            if numPartitions is None:
                if src is None:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(client.getDataFrameService().groupByKey(self._id))
                else:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().groupByKey2b(self._id, ISource.wrap(src).rpc()))
            else:
                if src is None:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(client.getDataFrameService().groupByKey2a(self._id, numPartitions))
                else:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().groupByKey3(self._id, numPartitions, ISource.wrap(src).rpc()))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def reduceByKey(self, src, numPartitions=None, localReduce=True):
        try:
            if numPartitions is None:
                with Ignis._clientPool().getClient() as client:
                    return IPairDataFrame(client.getDataFrameService().reduceByKey(self._id, ISource.wrap(src).rpc(),
                                                                                   localReduce))
            else:
                with Ignis._clientPool().getClient() as client:
                    return IPairDataFrame(client.getDataFrameService().reduceByKey4(self._id, ISource.wrap(src).rpc(),
                                          numPartitions, localReduce))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def aggregateByKey(self, zero, seqOp, combOp=None, numPartitions=None):
        try:
            if numPartitions is None:
                if combOp is None:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().aggregateByKey(self._id,
                                                                        ISource.wrap(zero).rpc(),
                                                                        ISource.wrap(seqOp).rpc()))
                else:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().aggregateByKey4b(self._id,
                                                                          ISource.wrap(zero).rpc(),
                                                                          ISource.wrap(seqOp).rpc(),
                                                                          ISource.wrap(combOp).rpc()))
            else:
                if combOp is None:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().aggregateByKey4a(self._id,
                                                                          ISource.wrap(zero).rpc(),
                                                                          ISource.wrap(seqOp).rpc(),
                                                                          numPartitions))
                else:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().aggregateByKey5(self._id,
                                                                         ISource.wrap(zero).rpc(),
                                                                         ISource.wrap(seqOp).rpc(),
                                                                         ISource.wrap(combOp).rpc(),
                                                                         numPartitions))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def foldByKey(self, zero, src, numPartitions=None, localFold=True):
        try:
            if numPartitions is None:
                with Ignis._clientPool().getClient() as client:
                    return IPairDataFrame(
                        client.getDataFrameService().foldByKey(self._id,
                                                               ISource.wrap(zero).rpc(),
                                                               ISource.wrap(src).rpc(),
                                                               localFold))
            else:
                with Ignis._clientPool().getClient() as client:
                    return IPairDataFrame(
                        client.getDataFrameService().foldByKey5(self._id,
                                                                ISource.wrap(zero).rpc(),
                                                                ISource.wrap(src).rpc(),
                                                                numPartitions,
                                                                localFold))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def sortByKey(self, ascending=True, numPartitions=None, src=None):
        try:
            if numPartitions is None:
                if src is None:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().sortByKey(self._id,
                                                                   ascending))
                else:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().sortByKey3b(self._id,
                                                                     ascending,
                                                                     ISource.wrap(src).rpc()))
            else:
                if src is None:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().sortByKey3a(self._id,
                                                                     ascending,
                                                                     numPartitions))
                else:
                    with Ignis._clientPool().getClient() as client:
                        return IPairDataFrame(
                            client.getDataFrameService().sortByKey4(self._id,
                                                                    ascending,
                                                                    numPartitions,
                                                                    ISource.wrap(src).rpc()))

        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def keys(self):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect(
                    client.getDataFrameService().keys(self._id,
                                                      ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def values(self):
        try:
            with Ignis._clientPool().getClient() as client:
                return Ignis._driverContext().collect(
                    client.getDataFrameService().values(self._id,
                                                        ISource("").rpc())
                )
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def sampleByKey(self, withReplacement, fractions, seed, native=False):
        try:
            fractions_src = ISource("", native)
            fractions_src.addParam("fractions", fractions)
            with Ignis._clientPool().getClient() as client:
                return IPairDataFrame(
                    client.getDataFrameService().sampleByKey(self._id,
                                                             withReplacement,
                                                             fractions_src.rpc(),
                                                             seed))
        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def countByKey(self):
        try:
            with Ignis._clientPool().getClient() as client:
                counts = Ignis._driverContext().collect(
                    client.getDataFrameService().countByKey(self._id,
                                                            ISource("").rpc())
                )
            i = 1
            while i < len(counts):
                for key, value in counts[i]:
                    if key in counts[0]:
                        counts[0][key] += counts[i][key]
                    else:
                        counts[0][key] = counts[i][key]
                i += 1
            return counts[0]

        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)

    def countByValue(self):
        try:
            with Ignis._clientPool().getClient() as client:
                counts = Ignis._driverContext().collect(
                    client.getDataFrameService().countByValue(self._id,
                                                              ISource("").rpc())
                )
            i = 1
            while i < len(counts):
                for key, value in counts[i]:
                    if key in counts[0]:
                        counts[0][key] += counts[i][key]
                    else:
                        counts[0][key] = counts[i][key]
                i += 1
            return counts[0]

        except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
            raise IDriverException(ex.message, ex.cause_)
