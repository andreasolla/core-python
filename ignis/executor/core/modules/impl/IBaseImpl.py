from ignis.executor.core.storage import IMemoryPartition
from ignis.executor.core.IMpi import MPI


class IBaseImpl:
    def __init__(self, executor_data, logger):
        self._executor_data = executor_data
        self.__logger = logger

    def resizeMemoryPartition(self, part, n):
        inner = part._inner()
        cls = part._IMemoryPartition__cls
        if cls == bytearray or cls == list:
            part._IMemoryPartition__elements = inner[0:n]
        elif cls.__name__ == 'INumpyWrapper':
            part.array = part.array[0:n]
            part._INumpy__next = n
        else:
            newPart = IMemoryPartition(part._native, cls)
            writer = newPart.writeIterator()
            it = part.readIterator()
            for i in range(n):
                writer.write(it.next())

    def exchange(self, input, output):
        executors = self._executor_data.mpi().executors()
        if executors == 1:
            for part in input:
                part.fit()
                output.add(part)
            return

        tp = self._executor_data.getProperties().exchangeType()
        sync = None
        if tp == "sync":
            sync = True
        elif tp == "async":
            sync = False
        else:
            self.__logger.info("Base: detecting exchange type")
            data = [len(input), sum(1 for p in input if p.empty())]
            data = self._executor_data.mpi().native().reduce(data, MPI.SUM)
            if self._executor_data.mpi().isRoot(0):
                n = data[0]
                n_zero = data[1]
                sync = n_zero < int(n / executors)
            sync = self._executor_data.mpi().native().bcast(sync, 0)

        if sync:
            self.__logger.info("Base: using synchronous exchange")
            self._executor_data.mpi().exchange_sync(input, output)
        else:
            self.__logger.info("Base: using asynchronous exchange")
            self._executor_data.mpi().exchange_async(input, output)
