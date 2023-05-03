import logging
import math

from ignis.executor.core.IMpi import MPI
from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl

logger = logging.getLogger(__name__)


def cmp_to_key(cmp):
    class K(object):
        __slots__ = ['obj']

        def __init__(self, obj, *args):
            self.obj = obj

        def __lt__(self, other):
            return cmp(self.obj, other.obj)

    return K


class ISortImpl(IBaseImpl):

    def __init__(self, executor_data):
        IBaseImpl.__init__(self, executor_data, logger)

    def sort(self, ascending, numPartitions=-1):
        self.__sortImpl(None, ascending, numPartitions)

    def sortBy(self, f, ascending, numPartitions=-1):
        context = self._executor_data.getContext()
        f.before(context)
        self.__sortImpl(lambda a, b: f.call(a, b, context), ascending, numPartitions)
        f.after(context)

    def top(self, num, cmp=None):
        if cmp is None:
            self.__take_ordered_impl(comparator=None, ascending=False, n=num)
        else:
            context = self._executor_data.getContext()
            cmp.before(context)
            self.__take_ordered_impl(comparator=lambda a, b: cmp.call(a, b, context), ascending=False, n=num)
            cmp.after(context)

    def takeOrdered(self, num, cmp=None):
        if cmp is None:
            self.__take_ordered_impl(comparator=None, ascending=True, n=num)
        else:
            context = self._executor_data.getContext()
            cmp.before(context)
            self.__take_ordered_impl(comparator=lambda a, b: cmp.call(a, b, context), ascending=True, n=num)
            cmp.after(context)

    def sortByKey(self, ascending, numPartitions=-1):
        self.__sortImpl(lambda a, b: a[0] < b[0], ascending, numPartitions)

    def sortByKeyBy(self, f, ascending, numPartitions=-1):
        context = self._executor_data.getContext()
        f.before(context)
        self.__sortImpl(lambda a, b: f.call(a[0], b[0], context), ascending, numPartitions)
        f.after(context)

    def max(self, cmp=None):
        if cmp is None:
            self.__max_impl(comparator=None, ascending=False)
        else:
            context = self._executor_data.getContext()
            cmp.before(context)
            self.__max_impl(comparator=lambda a, b: cmp.call(a, b, context), ascending=False)
            cmp.after(context)

    def min(self, cmp=None):
        if cmp is None:
            self.__max_impl(comparator=None, ascending=True)
        else:
            context = self._executor_data.getContext()
            cmp.before(context)
            self.__max_impl(comparator=lambda a, b: cmp.call(a, b, context), ascending=True)
            cmp.after(context)

    def __sortImpl(self, cmp, ascending, partitions, local_sort=True):
        input = self._executor_data.getPartitions()
        executors = self._executor_data.mpi().executors()
        # Copy the data if they are reused
        if input.cache():  # Work directly on the array to improve performance
            if self._executor_data.getPartitionTools().isMemory(input):
                input = input.clone()
            else:
                # Only group will be affected
                input = input.shadowCopy()
        # Sort each partition
        if local_sort:
            logger.info("Sort: sorting " + str(len(input)) + " partitions locally")
            self.__localSort(input, cmp, ascending)

        localPartitions = input.partitions()
        totalPartitions = self._executor_data.mpi().native().allreduce(localPartitions, MPI.SUM)
        if totalPartitions < 2:
            self._executor_data.setPartitions(input)
            return
        if partitions < 0:
            partitions = totalPartitions

        # Generates pivots to separate the elements in order
        sr = self._executor_data.getProperties().sortSamples()
        if sr > 1:
            samples = int(sr)
        else:
            send = [len(input), sum(map(len, input))]
            rcv = self._executor_data.mpi().native().allreduce(send, MPI.SUM)
            samples = math.ceil(rcv[1] / rcv[0] * sr)

        samples = max(partitions, samples)
        logger.info("Sort: selecting " + str(samples) + " pivots")
        pivots = self.__selectPivots(input, samples)

        resampling = self._executor_data.getProperties().sortResampling()
        if sr < 1 and resampling and executors > 1 and local_sort:
            logger.info("Sort: -- resampling pivots begin --")
            tmp = self._executor_data.getPartitionTools().newPartitionGroup(0)
            tmp.add(pivots)
            self._executor_data.setPartitions(tmp)
            self.__sortImpl(cmp, ascending, partitions, False)
            logger.info("Sort: -- resampling pivots end --")

            samples = partitions - 1
            logger.info("Sort: selecting " + str(samples) + " partition pivots")
            pivots = self.__parallelSelectPivots(samples)

            logger.info("Sort: collecting pivots")
            self._executor_data.mpi().gather(pivots, 0)
        else:
            logger.info("Sort: collecting pivots")
            self._executor_data.mpi().gather(pivots, 0)

            if self._executor_data.mpi().isRoot(0):
                group = self._executor_data.getPartitionTools().newPartitionGroup(0)
                group.add(pivots)
                self.__localSort(group, cmp, ascending)
                samples = partitions - 1

                logger.info("Sort: selecting " + str(samples) + " partition pivots")
                pivots = self.__selectPivots(group, samples)

        logger.info("Sort: broadcasting pivots ranges")
        self._executor_data.mpi().bcast(pivots, 0)

        ranges = self.__generateRanges(input, pivots, cmp, ascending)
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        logger.info("Sort: exchanging ranges")
        self.exchange(ranges, output)

        # Sort final partitions
        logger.info("Sort: sorting again " + str(len(output)) + " partitions locally")
        self.__localSort(output, cmp, ascending)
        self._executor_data.setPartitions(output)

    def __localSort(self, group, cmp, ascending):
        inMemory = self._executor_data.getPartitionTools().isMemory(group)

        for i in range(0, len(group)):
            part = group[i]
            if not inMemory:
                new_part = self._executor_data.getPartitionTools().newMemoryPartition(part.size())
                part.moveTo(new_part)
                part = new_part

            elems = part._IMemoryPartition__elements

            if isinstance(elems, bytearray):
                if cmp:
                    part._IMemoryPartition__elements = bytearray(
                        sorted(elems, key=cmp_to_key(cmp), reverse=not ascending))
                else:
                    part._IMemoryPartition__elements = bytearray(sorted(elems, reverse=not ascending))
            elif type(elems).__name__ == 'INumpyWrapper':
                if cmp:
                    import numpy
                    elems.array = numpy.array(sorted(elems.usedArray(), key=cmp_to_key(cmp), reverse=not ascending))
                else:
                    if ascending:
                        elems.usedArray().sort()
                    else:
                        elems.usedArray()[::-1].sort()
            else:
                if cmp:
                    elems.sort(key=cmp_to_key(cmp), reverse=not ascending)
                else:
                    elems.sort(reverse=not ascending)

            if not inMemory:
                group[i] = self._executor_data.getPartitionTools().newPartition()
                part.moveTo(group[i])

    def __selectPivots(self, group, samples):
        pivots = self._executor_data.getPartitionTools().newMemoryPartition()
        inMemory = self._executor_data.getPartitionTools().isMemory(group)
        writer = pivots.writeIterator()
        for part in group:
            if len(part) < samples:
                part.copyTo(pivots)
                writer = pivots.writeIterator()
                continue

            skip = int((len(part) - samples) / (samples + 1))
            rem = (len(part) - samples) % (samples + 1)
            if inMemory:
                pos = skip + (1 if rem > 0 else 0)
                for n in range(0, samples):
                    writer.write(part[pos])
                    if n < rem - 1:
                        pos += skip + 2
                    else:
                        pos += skip + 1
            else:
                reader = part.readIterator()
                for n in range(0, samples):
                    for i in range(skip):
                        reader.next()
                    if n < rem:
                        reader.next()
                    writer.write(reader.next())

        return pivots

    def __parallelSelectPivots(self, samples):
        rank = self._executor_data.mpi().rank()
        result = self._executor_data.getPartitionTools().newMemoryPartition(samples)
        tmp = self._executor_data.getPartitions()
        sz = sum(map(lambda p: len(p), tmp))

        aux = self._executor_data.mpi().native().allgather(sz)

        sz = sum(aux)
        disp = sum([aux[i] for i in range(rank)])

        skip = int((sz - samples) / (samples + 1))
        rem = (sz - samples) % (samples + 1)

        pos = skip + (1 if rem > 0 else 0)
        for sample in range(0, samples):
            if pos >= disp:
                break
            if sample < rem - 1:
                pos += skip + 2
            else:
                pos += skip + 1
        pos -= disp

        writer = result.writeIterator()
        for part in tmp:
            while pos < len(part) and sample < samples:
                writer.write(part[pos])
                if sample < rem - 1:
                    pos += skip + 2
                else:
                    pos += skip + 1
                sample +=1
            pos -= len(part)

        return result

    def __generateRanges(self, input, pivots, cmp, ascending):
        if cmp is None:
            cmp = lambda a, b: a < b
        if self._executor_data.getPartitionTools().isMemory(input):
            return self.__generateMemoryRanges(input, pivots, cmp, ascending)
        ranges = self._executor_data.getPartitionTools().newPartitionGroup(len(pivots) + 1)
        writers = [p.writeIterator() for p in ranges]

        for p in range(len(input)):
            for elem in input[p]:
                writers[self.__searchRange(elem, pivots, cmp, ascending)].write(elem)
            input[p] = None

        input.clear()
        return ranges

    def __generateMemoryRanges(self, input, pivots, cmp, ascending):
        ranges = self._executor_data.getPartitionTools().newPartitionGroup(len(pivots) + 1)
        writers = [p.writeIterator() for p in ranges]

        for p in range(len(input)):
            part = input[p]
            elems_stack = list()
            ranges_stack = list()

            elems_stack.append((0, len(part) - 1))
            ranges_stack.append((0, len(pivots)))

            while len(elems_stack) > 0:
                (start, end) = elems_stack.pop()
                mid = int((start + end) / 2)

                (first, last) = ranges_stack.pop()

                r = self.__searchRange(part[mid], pivots, cmp, ascending)
                writers[r].write(part[mid])

                if first == r:
                    for i in range(start, mid):
                        writers[r].write(part[i])
                elif start < mid:
                    elems_stack.append((start, mid - 1))
                    ranges_stack.append((first, r))

                if r == last:
                    for i in range(mid + 1, end + 1):
                        writers[r].write(part[i])
                elif mid < end:
                    elems_stack.append((mid + 1, end))
                    ranges_stack.append((r, last))

            input[p] = None
        return ranges

    def __searchRange(self, elem, pivots, cmp, ascending):
        start = 0
        end = len(pivots) - 1
        while start < end:
            mid = int((start + end) / 2)
            if cmp(elem, pivots[mid]) == ascending:
                end = mid - 1
            else:
                start = mid + 1
        if cmp(elem, pivots[start]) == ascending:
            return start
        else:
            return start + 1

    def __take_ordered_impl(self, comparator, ascending, n):
        input = self._executor_data.getPartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup()

        logger.info("Sort: top/takeOrdered " + str(n) + " elemens")
        top = self._executor_data.getPartitionTools().newMemoryPartition(n * len(input))
        logger.info("Sort: local partition top/takeOrdered")
        cmp = comparator if comparator is not None else lambda a, b: a < b
        for part in input:
            for item in part:
                self.__take_ordered_add(cmp, ascending, top, item, n)

        # logger.info("Sort: local executor top/takeOrdered")
        logger.info("Sort: global top/takeOrdered")
        self._executor_data.mpi().gather(top, 0)

        if self._executor_data.mpi().isRoot(0):
            tmp = self._executor_data.getPartitionTools().newPartitionGroup()
            tmp.add(top)
            self.__localSort(tmp, comparator, ascending)
            self.resizeMemoryPartition(top, n)
            output.add(top)

        self._executor_data.setPartitions(output)

    def __take_ordered_add(self, comparator, ascending, top, elem, n):
        inner = top._inner()
        if len(inner) == 0:
            inner.append(elem)
            return
        if len(inner) == n:
            if comparator(inner[-1], elem) == ascending or not (comparator(elem, inner[-1]) == ascending):
                return
            i = self.__searchRange(elem, top, comparator, ascending)
        else:
            i = self.__searchRange(elem, top, comparator, ascending)
            inner.append(elem)

        inner[i + 1:len(inner)] = inner[i:len(inner) - 1]
        inner[i] = elem

    def __max_impl(self, comparator, ascending):
        input = self._executor_data.getPartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup()

        logger.info("Sort: max/min")
        elem = None
        for part in input:
            if part:
                elem = part.readIterator().next()
                break

        if elem is None:
            self._executor_data.setPartitions(output)
            return

        if comparator is None:
            comparator = lambda a, b: a < b

        logger.info("Sort: local max/min")
        for part in input:
            for item in part:
                if comparator(item, elem) == ascending:
                    elem = item

        logger.info("Sort: global max/min")
        result = self._executor_data.getPartitionTools().newMemoryPartition()
        result.writeIterator().write(elem)
        self._executor_data.mpi().gather(result, 0)
        if self._executor_data.mpi().isRoot(0):
            for item in result:
                if comparator(item, elem) == ascending:
                    elem = item
            result.clear()
            result.writeIterator().write(elem)
            output.add(result)

        self._executor_data.setPartitions(output)
