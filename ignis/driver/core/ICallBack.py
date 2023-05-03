import threading

from ignis.driver.core.IDriverContext import IDriverContext
from ignis.executor.core import ILog
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.executor.core.modules.ICommModule import ICommModule
from ignis.executor.core.modules.IExecutorServerModule import IExecutorServerModule
from ignis.executor.core.modules.IIOModule import IIOModule
from ignis.rpc.executor.cachecontext.ICacheContextModule import Processor as ICacheContextModuleProcessor
from ignis.rpc.executor.comm.ICommModule import Processor as ICommModuleProcessor
from ignis.rpc.executor.io.IIOModule import Processor as IIOModuleProcessor


class ICallBack:

    def __init__(self, port, compression):
        ILog.init()
        self.__port = port
        self.__compression = compression

        class IExecutorServerModuleImpl(IExecutorServerModule):

            def __init__(self, executor_data, driverContext):
                IExecutorServerModule.__init__(self, executor_data)
                self.__driverContext = driverContext

            def _createServices(self, processor):
                io = IIOModule(self._executor_data)
                processor.registerProcessor("IIO", IIOModuleProcessor(io))
                processor.registerProcessor("ICacheContext", ICacheContextModuleProcessor(self.__driverContext))
                comm = ICommModule(self._executor_data)
                processor.registerProcessor("IComm", ICommModuleProcessor(comm))

        executor_data = IExecutorData()
        self.__driverContext = IDriverContext(executor_data)
        self.__server = IExecutorServerModuleImpl(executor_data, self.__driverContext)
        threading.Thread(target=IExecutorServerModuleImpl.serve,
                         args=(self.__server, "IExecutorServer", port, compression, True),
                         daemon=True).start()

    def stop(self):
        self.__server.stop()

    def driverContext(self):
        return self.__driverContext
