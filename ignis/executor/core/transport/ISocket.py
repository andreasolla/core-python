import errno
import socket
import sys

from thrift.transport.TSocket import TSocket, TServerSocket, TTransportException


class ISocket(TSocket):

	def __init__(self, host='localhost', port=9090, unix_socket=None, socket_family=socket.AF_UNSPEC):
		super().__init__(host, port, unix_socket, socket_family)

	def read(self, sz):
		try:
			buff = self.handle.recv(sz)
		except socket.error as e:
			if (e.args[0] == errno.ECONNRESET and (sys.platform == 'darwin' or sys.platform.startswith('freebsd'))):
				# freebsd and Mach don't follow POSIX semantic of recv
				# and fail with ECONNRESET if peer performed shutdown.
				# See corresponding comment and code in TSocket::read()
				# in lib/cpp/src/transport/TSocket.cpp.
				self.close()
				# Trigger the check to raise the END_OF_FILE exception below.
				buff = b''
			elif e.args[0] == errno.ETIMEDOUT:
				raise TTransportException(type=TTransportException.TIMED_OUT, message="read timeout", inner=e)
			else:
				raise TTransportException(message="unexpected exception", inner=e)
		return buff


class IServerSocket(TServerSocket):

	def __init__(self, host=None, port=9090, unix_socket=None, socket_family=socket.AF_UNSPEC):
		super().__init__(host, port, unix_socket, socket_family)

	def accept(self):
		client, addr = self.handle.accept()
		result = ISocket()
		result.setHandle(client)
		return result
