import logging
import os

from ignis.executor.api.IContext import IContext
from ignis.executor.core.ILibraryLoader import ILibraryLoader
from ignis.executor.core.IMpi import IMpi
from ignis.executor.core.IPartitionTools import IPartitionTools
from ignis.executor.core.IPropertyParser import IPropertyParser
from ignis.rpc.source.ttypes import ISource, IEncoded
from ignis.executor.core.transport.IBytesTransport import IBytesTransport
from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol

logger = logging.getLogger(__name__)


class IExecutorData:

    def __init__(self):
        self.__context = IContext()
        self.__properties = IPropertyParser(self.__context.props())
        self.__library_loader = ILibraryLoader(self.__properties)
        self.__partition_tools = IPartitionTools(self.__properties, self.__context)
        self.__mpi = IMpi(self.__properties, self.__partition_tools, self.__context)
        self.__partitions = None
        self.__variables = dict()

    def getPartitions(self):
        group = self.__partitions
        if len(group) > 0 and self.__properties.loadType():
            part = group[0]
            if self.__partition_tools.isMemory(part):
                cls = part._IMemoryPartition__cls
                self.__context.vars()["STORAGE_CLASS"] = cls
                if cls.__name__ == "INumpyWrapper":
                    self.__context.vars()["STORAGE_CLASS_DTYPE"] = part._IMemoryPartition__elements.array.dtype
        return group

    def getAndDeletePartitions(self):
        group = self.getPartitions()
        self.deletePartitions()
        if group.cache():
            return group.shadowCopy(self)
        return group

    def setPartitions(self, group):
        old = self.__partitions
        self.__partitions = group
        return old

    def hasPartitions(self):
        return self.__partitions is not None

    def deletePartitions(self):
        self.__partitions = None

    def setVariable(self, key, value):
        self.__variables[key] = value

    def getVariable(self, key):
        return self.__variables[key]

    def hasVariable(self, key):
        return key in self.__variables

    def removeVariable(self, key):
        if key in self.__variables:
            del self.__variables[key]

    def clearVariables(self):
        self.__variables.clear()

    def infoDirectory(self):
        info = self.__properties.executorDirectory() + "/info"
        self.__partition_tools.createDirectoryIfNotExists(info)
        return info

    def loadLibraryFunctions(self, path):
        self.__library_loader.loadLibrary(path)

    def loadLibrary(self, source, withBackup=True):
        logger.info("Loading function")
        if source.obj.bytes is not None:
            lib = self.__library_loader.unpickle(source.obj.bytes)
        else:
            lib = self.__library_loader.loadFuntion(source.obj.name)

        if source.params:
            logger.info("Loading user variables")
            self.loadParameters(source)

        if source.obj.name and ":" in source.obj.name and withBackup:
            with open(self.infoDirectory() + "/sources" + str(self.__context.executorId()) + ".bak", "a+") as file:
                file.write(source.obj.name)

        logger.info("Function loaded")
        return lib

    def loadParameters(self, source):
        for key, value in source.params.items():
            buffer = IBytesTransport(value)
            proto = IObjectProtocol(buffer)
            self.__context.vars()[key] = proto.readObject()

    def reloadLibraries(self):
        backup_path = self.infoDirectory() + "/sources" + str(self.__context.executorId()) + ".bak"
        loaded = set()
        source = ISource(obj=IEncoded())
        if os.path.exists(backup_path):
            logger.info("Function backup found, loading")
            with open(backup_path, "r") as file:
                for line in file:
                    try:
                        source.obj.name = line.rstrip()
                        if source.obj.name not in loaded:
                            self.loadLibrary(source, False)
                            loaded.add(source.obj.name)
                    except Exception as ex:
                        logger.info(str(ex))

    def getContext(self):
        return self.__context

    def getProperties(self):
        return self.__properties

    def getPartitionTools(self):
        return self.__partition_tools

    def mpi(self):
        return self.__mpi

    def setMpiGroup(self, group):
        self.__context._mpi_group = group
