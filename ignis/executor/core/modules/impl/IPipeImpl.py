import logging

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl, IMemoryPartition

logger = logging.getLogger(__name__)


class IPipeImpl(IBaseImpl):

    def __init__(self, executor_data):
        IBaseImpl.__init__(self, executor_data, logger)

    def execute(self, f):
        context = self._executor_data.getContext()
        f.before(context)
        logger.info("General: Execute")
        f.call(context)
        f.after(context)

    def executeTo(self, f):
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        context = self._executor_data.getContext()
        f.before(context)
        logger.info("General: mapExecuteTo")

        newParts = f.call(context)
        logger.info("General: moving elements to partitions")
        for v in newParts:
            part = self._executor_data.getPartitionTools().newMemoryPartition(0)
            part._IMemoryPartition__elements = v
            output.add(part)

        if self._executor_data.getProperties().partitionType() != IMemoryPartition.TYPE:
            logger.info("General: saving partitions from memory")
            aux = self._executor_data.getPartitionTools().newPartitionGroup()
            for men in output:
                part = self._executor_data.getPartitionTools().newPartition(part)
                men.copyTo(part)
                aux.add(part)
            output = aux
        f.after(context)
        self._executor_data.setPartitions(output)

    def map(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: map " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in input[i]:
                it.write(f.call(elem, context))
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)

    def filter(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: filter " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in input[i]:
                if f.call(elem, context):
                    it.write(elem)
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)

    def flatmap(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: flatmap " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in input[i]:
                for elem2 in f.call(elem, context):
                    it.write(elem2)
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)

    def keyBy(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: keyBy " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in input[i]:
                it.write((f.call(elem, context), elem))
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)

    def mapWithIndex(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: mapWithIndex " + str(len(input)) + " partitions")

        elems = sum([len(p) for p in input])
        indices = self._executor_data.mpi().native().allgather(elems)
        id = sum(indices[0:self._executor_data.mpi().rank()])

        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in input[i]:
                it.write(f.call(id, elem, context))
                id += 1
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)

    def mapPartitions(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: mapPartitions " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in f.call(input[i].readIterator(), context):
                it.write(elem)
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)

    def mapPartitionsWithIndex(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: mapPartitionsWithIndex " + str(len(input)) + " partitions")

        indices = self._executor_data.mpi().native().allgather(len(input))
        offset = sum(indices[0:self._executor_data.mpi().rank()])

        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in f.call(offset + i, input[i].readIterator(), context):
                it.write(elem)
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)

    def mapExecutor(self, f):
        input = self._executor_data.getPartitions()
        inMemory = self._executor_data.getPartitionTools().isMemory(input)
        context = self._executor_data.getContext()
        f.before(context)
        logger.info("General: mapExecutor " + str(len(input)) + " partitions")
        if not inMemory or input.cache():
            logger.info("General: loading partitions in memory")
            aux = self._executor_data.getPartitionTools().newPartitionGroup()
            for part in input:
                men = self._executor_data.getPartitionTools().newMemoryPartition(len(part))
                part.copyTo(men)
                aux.add(men)
            input = aux

        arg = list()
        for part in input:
            arg.append(part._inner())

        f.call(arg, context)

        if not inMemory:
            logger.info("General: saving partitions from memory")
            aux = self._executor_data.getPartitionTools().newPartitionGroup()
            for men in input:
                part = self._executor_data.getPartitionTools().newPartition(men)
                men.copyTo(part)
                aux.add(part)
            input = aux
        f.after(context)
        self._executor_data.setPartitions(input)

    def mapExecutorTo(self, f):
        input = self._executor_data.getPartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        inMemory = self._executor_data.getPartitionTools().isMemory(input)
        context = self._executor_data.getContext()
        f.before(context)
        logger.info("General: mapExecutorTo " + str(len(input)) + " partitions")
        if not inMemory or input.cache():
            logger.info("General: loading partitions in memory")
            aux = self._executor_data.getPartitionTools().newPartitionGroup()
            for part in input:
                men = self._executor_data.getPartitionTools().newMemoryPartition(len(part))
                part.copyTo(men)
                aux.add(men)
            input = aux

        arg = list()
        for part in input:
            arg.append(part._inner())

        newParts = f.call(arg, context)
        logger.info("General: moving elements to partitions")
        for v in newParts:
            part = self._executor_data.getPartitionTools().newMemoryPartition(0)
            part._IMemoryPartition__elements = v
            output.add(part)

        if not inMemory:
            logger.info("General: saving partitions from memory")
            aux = self._executor_data.getPartitionTools().newPartitionGroup()
            for men in output:
                part = self._executor_data.getPartitionTools().newPartition(part)
                men.copyTo(part)
                aux.add(part)
            output = aux
        f.after(context)
        self._executor_data.setPartitions(output)

    def foreach(self, f):
        input = self._executor_data.getAndDeletePartitions()
        context = self._executor_data.getContext()

        f.before(context)
        logger.info("General: foreach " + str(len(input)) + " partitions")
        for i in range(len(input)):
            for elem in input[i]:
                f.call(elem, context)
            input[i] = None
        f.after(context)
        self._executor_data.deletePartitions()

    def foreachPartition(self, f):
        input = self._executor_data.getAndDeletePartitions()
        context = self._executor_data.getContext()

        f.before(context)
        logger.info("General: foreachPartition " + str(len(input)) + " partitions")
        for i in range(len(input)):
            reader = input[i].readIterator()
            f.call(reader, context)
            input[i] = None
        f.after(context)
        self._executor_data.deletePartitions()

    def foreachExecutor(self, f):
        input = self._executor_data.getPartitions()
        inMemory = self._executor_data.getPartitionTools().isMemory(input)
        context = self._executor_data.getContext()
        f.before(context)
        logger.info("General: foreachExecutor " + str(len(input)) + " partitions")
        if not inMemory:
            logger.info("General: loading partitions in memory")
            aux = self._executor_data.getPartitionTools().newPartitionGroup()
            for part in input:
                men = self._executor_data.getPartitionTools().newMemoryPartition(len(part))
                part.copyTo(men)
                aux.add(men)
            input = aux

        arg = list()
        for part in input:
            arg.append(part._inner())

        f.call(arg, context)

    def take(self, num):
        input = self._executor_data.getPartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        count = 0
        for part in input:
            if len(part) + count > num:
                cut = self._executor_data.getPartitionTools().newPartition(part.type())
                reader = part.readIterator()
                writer = cut.writeIterator()
                while count != num:
                    count += 1
                    writer.write(reader.next())
                output.add(cut)
                break
            count += len(part)
            output.add(part)
        self._executor_data.setPartitions(output)

    def keys(self):
        input = self._executor_data.getAndDeletePartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: keys " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in input[i]:
                it.write(elem[0])
            input[i]=None

        self._executor_data.setPartitions(output)

    def values(self):
        input = self._executor_data.getAndDeletePartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: values " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for elem in input[i]:
                it.write(elem[1])
            input[i] = None

        self._executor_data.setPartitions(output)

    def flatMapValues(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: flatMapValues " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for key, value in input[i]:
                for value2 in f.call(value, context):
                    it.write((key, value2))
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)

    def mapValues(self, f):
        context = self._executor_data.getContext()
        input = self._executor_data.getAndDeletePartitions()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup(input)
        logger.info("General: flatMapValues " + str(len(input)) + " partitions")
        for i in range(len(input)):
            it = output[i].writeIterator()
            for key, value in input[i]:
                it.write((key, f.call(value, context)))
            input[i] = None
        f.after(context)
        self._executor_data.setPartitions(output)
