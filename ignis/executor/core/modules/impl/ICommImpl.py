import logging
from ctypes import c_longlong, c_bool

from ignis.executor.core.IMpi import MPI
from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl
from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol
from ignis.executor.core.storage import IMemoryPartition
from ignis.executor.core.transport.IBytesTransport import IBytesTransport
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer
from ignis.executor.core.transport.IZlibTransport import IZlibTransport

logger = logging.getLogger(__name__)


class ICommImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data, logger)
		self.__groups = dict()

	def openGroup(self):
		logger.info("Comm: creating group")
		port_name = MPI.Open_port()

		class Handle:
			def __del__(self):  # Close port on context clear
				MPI.Close_port(port_name)

		logger.info("Comm: group created on " + str(port_name))
		self._executor_data.setVariable("server", Handle())
		return port_name

	def closeGroup(self):
		logger.info("Comm: closing group server")
		self._executor_data.removeVariable("server")

	def joinToGroup(self, id, leader):
		self._executor_data.setMpiGroup(self.__joinToGroupImpl(id, leader))

	def joinToGroupName(self, id, leader, name):
		self.__groups[name] = self.__joinToGroupImpl(id, leader)

	def hasGroup(self, name):
		return name in self.__groups

	def destroyGroup(self, name):
		if len(name) == 0:
			comm = self._executor_data.mpi().native()
			if comm != MPI.COMM_WORLD:
				comm.Free()
				self._executor_data.setMpiGroup(MPI.COMM_WORLD)
		elif name in self.__groups:
			comm = self.__groups[name]
			comm.Free()
			del self.__groups[name]

	def destroyGroups(self):
		for name, comm in self.__groups:
			comm.Free()
		self.__groups.clear()
		self.destroyGroup("")

	def getProtocol(self):
		return IObjectProtocol.PYTHON_PROTOCOL

	def getPartitions(self, protocol, minPartitions=1):
		partitions = list()
		group = self._executor_data.getPartitions()
		cmp = self._executor_data.getProperties().msgCompression()
		native = self.getProtocol() == protocol and self._executor_data.getProperties().nativeSerialization()
		buffer = IMemoryBuffer()
		if len(group) > minPartitions:
			for part in group:
				buffer.resetBuffer()
				part.write(buffer, cmp, native)
				partitions.append(buffer.getBufferAsBytes())
		elif len(group) == 1 and self._executor_data.getPartitionTools().isMemory(group):
			men = group[0]
			zlib = IZlibTransport(buffer, cmp)
			proto = IObjectProtocol(zlib)
			partition_elems = int(len(men) / minPartitions)
			remainder = len(men) % minPartitions
			offset = 0
			for p in range(minPartitions):
				sz = partition_elems + (1 if p < remainder else 0)
				proto.writeObject(men._IMemoryPartition__elements[offset:offset + sz], native)
				offset += sz
				zlib.flush()
				zlib.reset()
				partitions.append(buffer.getBufferAsBytes())
				buffer.resetBuffer()

		elif len(group) > 0:
			elemens = 0
			for part in group:
				elemens += len(part)
			part = IMemoryPartition(1024 * 1024)
			partition_elems = int(elemens / minPartitions)
			remainder = elemens % minPartitions
			i = 0
			ew = 0
			er = 0
			it = group[0].readIterator()
			for p in range(minPartitions):
				part.clear()
				writer = part.writeIterator()
				ew = partition_elems
				if p < remainder:
					ew += 1

				while ew > 0:
					if er == 0:
						er = len(group[i])
						it = group[i].readIterator()
						i += 1
					n = min(ew, er)
					for _ in range(n):
						writer.write(it.next())
					ew -= n
					er -= n
				part.write(buffer, cmp, native)
				partitions.append(buffer.getBufferAsBytes())
				buffer.resetBuffer()
		else:
			part = IMemoryPartition(native=native)
			part.write(buffer, cmp, native)
			for _ in range(minPartitions):
				partitions.append(buffer.getBufferAsBytes())

		return partitions

	def setPartitions(self, partitions):
		group = self._executor_data.getPartitionTools().newPartitionGroup(len(partitions))
		for i in range(0, len(partitions)):
			group[i].read(IBytesTransport(partitions[i]))
		self._executor_data.setPartitions(group)

	def driverGather(self, group):
		comm = self.__getGroup(group)
		if comm.Get_rank() == 0:
			self._executor_data.setPartitions(self._executor_data.getPartitionTools().newPartitionGroup())
		self._executor_data.mpi().driverGather(comm, self._executor_data.getPartitions())

	def driverGather0(self, group):
		comm = self.__getGroup(group)
		if comm.Get_rank() == 0:
			self._executor_data.setPartitions(self._executor_data.getPartitionTools().newPartitionGroup())
		self._executor_data.mpi().driverGather0(comm, self._executor_data.getPartitions())

	def driverScatter(self, group, partitions):
		comm = self.__getGroup(group)
		if comm.Get_rank() != 0:
			self._executor_data.setPartitions(self._executor_data.getPartitionTools().newPartitionGroup())
		self._executor_data.mpi().driverScatter(comm, self._executor_data.getPartitions(), partitions)

	def importData(self, group, source, threads):
		comm = self.__getGroup(group)
		executors = comm.Get_size()
		me = comm.Get_rank()
		queue,  ranges = self.__importDataAux(comm, source)
		offset = ranges[me][0]
		if source:
			logger.info("General: importData sending partitions")
		else:
			logger.info("General: importData receiving partitions")

		shared = self._executor_data.getAndDeletePartitions() if source else \
			self._executor_data.getPartitionTools().newPartitionGroup(ranges[me][1] - ranges[me][0])

		for i in range(len(queue)):
			other = queue[i]
			if other == executors or other == me:
				continue
			init = max(ranges[me][0], ranges[other][0])
			end = min(ranges[me][1], ranges[other][1])
			if end - init < 1:
				continue

			opt = self._executor_data.mpi().getMsgOpt(comm, shared[0].type(), source, other, 0)
			for j in range(end - init):
				if source:
					self._executor_data.mpi().sendGroup(comm, shared[init - offset + j], other, 0, opt)
					shared[init - offset + j] = None
				else:
					self._executor_data.mpi().recvGroup(comm, shared[init - offset + j], other, 0, opt)

		if source:
			self._executor_data.deletePartitions()
		else:
			self._executor_data.setPartitions(shared)

	def __importDataAux(self, group, source):
		rank = group.Get_rank()
		local_rank = self._executor_data.mpi().rank()
		executors = group.Get_size()
		local_executors = self._executor_data.getContext().executors()
		remote_executors = executors - local_executors
		local_root = rank - local_rank
		remote_root = local_executors if local_root == 0 else 0
		source_root = local_root if source else remote_root
		target_executors = remote_executors if source else local_executors
		numPartitions = c_longlong(0)
		if source:
			input = self._executor_data.getPartitions()
			numPartitions.value = len(input)

		executors_count = (c_longlong * executors)()
		group.Allgather((numPartitions, 1, MPI.LONG_LONG), (executors_count, 1, MPI.LONG_LONG))
		numPartitions = sum(executors_count)
		logger.info("General: importData " + str(numPartitions) + "partitions")
		source_ranges = list()
		offset = 0

		for i in range(source_root, source_root + executors - target_executors):
			source_ranges.append((offset, offset + executors_count[i]))
			offset += executors_count[i]

		block = int(numPartitions / target_executors)
		remainder = numPartitions % target_executors
		target_ranges = list()
		for i in range(target_executors):
			if i < remainder:
				init = (block + 1) * i
				end = init + block + 1
			else:
				init = (block + 1) * remainder + block * (i - remainder)
				end = init + block
			target_ranges.append((init, end))

		if source_root == 0:
			ranges = source_ranges + target_ranges
		else:
			ranges = target_ranges + source_ranges

		global_queue = list()
		m = executors if executors % 2 == 0 else executors + 1
		id = 0
		id2 = m * m - 2
		for i in range(m - 1):
			if rank == id % (m - 1):
				global_queue.append(m - 1)
			if rank == m - 1:
				global_queue.append(id % (m - 1))
			id += 1
			for j in range(1, int(m / 2)):
				if rank == id % (m - 1):
					global_queue.append(id2 % (m - 1))
				if rank == id2 % (m - 1):
					global_queue.append(id % (m - 1))
				id += 1
				id2 -= 1
		queue = list()
		for other in global_queue:
			if local_root > 0:
				if other < local_root:
					queue.append(other)
			else:
				if other >= remote_root:
					queue.append(other)
		return queue, ranges

	def __joinToGroupImpl(self, id, leader):
		root = self._executor_data.hasVariable("server")
		comm = self._executor_data.mpi().native()
		if leader:
			port = id if root else None
			intercomm = comm.Accept(port)
		else:
			port = id
			intercomm = comm.Connect(port)
		comm1 = intercomm.Merge(not leader)
		intercomm.Free()
		return comm1

	def __getGroup(self, id):
		if id not in self.__groups:
			raise KeyError("Group " + id + " not found")
		return self.__groups[id]
