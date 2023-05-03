import threading

from ignis.driver.core.IClient import IClient


class IClientPool:
	def __init__(self, port, compression):
		self.__port = port
		self.__compression = compression
		self.__clients = list()
		self.__queue = list()
		self.__lock = threading.Lock()

	def destroy(self):
		with self.__lock:
			for client in self.__clients:
				client._close()
			self.__clients.clear()

	def getClient(self):
		return IClientPool.__ClientBound(self.__port, self.__compression, self.__clients, self.__queue, self.__lock)

	class __ClientBound:
		def __init__(self, port, compression, clients, queue, lock):
			self.__port = port
			self.__compression = compression
			self.__clients = clients
			self.__queue = queue
			self.__lock = lock
			self.__client = None

		def __enter__(self):
			with self.__lock:
				if self.__queue:
					self.__client = self.__queue.pop()
			if not self.__client:
				self.__client = IClient(self.__port, self.__compression)
				with self.__lock:
					self.__clients.append(self.__client)
			return self.__client

		def __exit__(self, exc_type, exc_val, exc_tb):
			with self.__lock:
				self.__queue.append(self.__client)
			self.__client = None
