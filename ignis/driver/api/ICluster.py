import ignis.rpc.driver.exception.ttypes
from ignis.driver.api.IDriverException import IDriverException
from ignis.driver.api.Ignis import Ignis


class ICluster:

	def __init__(self, properties=None, name=None):
		try:
			with Ignis._clientPool().getClient() as client:
				if properties is None:
					if name is None:
						self._id = client.getClusterService().newInstance0()
					else:
						self._id = client.getClusterService().newInstance1a(name)
				else:
					if name is None:
						self._id = client.getClusterService().newInstance1b(properties._id)
					else:
						self._id = client.getClusterService().newInstance2(name, properties._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex.cause_)

	def start(self):
		try:
			with Ignis._clientPool().getClient() as client:
				client.getClusterService().start(self._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex.cause_)

	def destroy(self):
		try:
			with Ignis._clientPool().getClient() as client:
				client.getClusterService().destroy(self._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex.cause_)

	def setName(self, name):
		try:
			with Ignis._clientPool().getClient() as client:
				client.getClusterService().setName(self._id, name)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex.cause_)

	def execute(self, *args):
		try:
			with Ignis._clientPool().getClient() as client:
				client.getClusterService().execute(self._id, args)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex.cause_)

	def executeScript(self, script):
		try:
			with Ignis._clientPool().getClient() as client:
				client.getClusterService().executeScript(self._id, script)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex.cause_)

	def sendFile(self, source, target):
		try:
			with Ignis._clientPool().getClient() as client:
				client.getClusterService().sendFile(self._id, source, target)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex.cause_)

	def sendCompressedFile(self, source, target):
		try:
			with Ignis._clientPool().getClient() as client:
				client.getClusterService().sendCompressedFile(self._id, source, target)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex.cause_)
