import logging
import os
import threading

from thrift.TMultiplexedProcessor import TMultiplexedProcessor
from thrift.protocol.TCompactProtocol import TCompactProtocolFactory
from thrift.server.TServer import TServer
from thrift.transport.TSocket import TServerSocket

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.IMpi import MPI
from ignis.executor.core.transport.IZlibTransport import TZlibTransportFactoryExt
from ignis.rpc.executor.server.IExecutorServerModule import Iface as IExecutorServerModuleIface, \
	Processor as IExecutorServerModuleProcessor

logger = logging.getLogger(__name__)


class IThreadedServer(TServer):

	def __init__(self, *args):
		TServer.__init__(self, *args)
		self.__stop = False
		self.__clients = list()

	def serve(self):
		self.__stop = False
		self.serverTransport.listen()
		try:
			while not self.__stop:
				try:
					client = self.serverTransport.accept()
					self.__clients.append(client)
					if not client:
						continue
					t = threading.Thread(target=self.handle, args=(client,))
					t.setDaemon(True)
					t.start()
				except KeyboardInterrupt:
					raise
				except Exception as x:
					if not self.__stop:
						logger.exception(x)
		finally:
			self.stop()
			self.serverTransport.close()

	def handle(self, client):
		trans = self.inputTransportFactory.getTransport(client)
		prot = self.inputProtocolFactory.getProtocol(trans)

		try:
			while not self.__stop:
				self.processor.process(prot, prot)
		except Exception as x:
			if not self.__stop:
				logger.exception(x)

		trans.close()

	def stop(self):
		for client in self.__clients:
			self.__stop = True
			client.close()


class IExecutorServerModule(IModule, IExecutorServerModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__server = None
		self.__processor = None

	def serve(self, name, port, compression, local_mode):
		if not self.__server:
			self.__processor = TMultiplexedProcessor()
			self.__server = IThreadedServer(
				self.__processor,
				TServerSocket(host='127.0.0.1' if local_mode else '0.0.0.0', port=port),
				TZlibTransportFactoryExt(compression),
				TCompactProtocolFactory()
			)

			self.__processor.registerProcessor(name, IExecutorServerModuleProcessor(self))
			logger.info("ServerModule: python executor started")
			self.__server.serve()
			logger.info("ServerModule: python executor stopped")
			self.stop()

	def start(self, properties, env):
		try:
			self._executor_data.getContext().props().update(properties)

			for key, value in env.items():
				os.environ[key] = value

			MPI.Init()
			logger.info("ServerModule: Mpi started")

			self._createServices(self.__processor)
			logger.info("ServerModule: python executor ready")
		except Exception as ex:
			self._pack_exception(ex)

	def stop(self):
		try:
			MPI.Finalize()
			if self.__server:
				aux = self.__server
				self.__server = None
				aux.stop()
		except Exception as ex:
			self._pack_exception(ex)

	def test(self):
		try:
			logger.info("ServerModule: test ok")
			return True
		except Exception as ex:
			self._pack_exception(ex)

	def _createServices(self, processor):
		pass
