import mpi4py


class IContext:

	def __init__(self):
		self.__properties = dict()
		self.__variables = dict()
		self._mpi_group = mpi4py.MPI.COMM_WORLD

	def cores(self):
		return 1

	def executors(self):
		return self._mpi_group.Get_size()

	def executorId(self):
		return self._mpi_group.Get_rank()

	def threadId(self):
		return 0

	def mpiGroup(self):
		return self._mpi_group

	def props(self):
		return self.__properties

	def vars(self):
		return self.__variables
