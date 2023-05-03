import logging
from ctypes import c_int, c_bool, c_byte, memmove

from mpi4py import rc

rc.initialize = False
rc.finalize = False
from mpi4py import MPI
import cloudpickle

from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol
from ignis.executor.core.storage import IMemoryPartition, IRawMemoryPartition, IDiskPartition
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer, IBuffer
from ignis.executor.core.transport.IZlibTransport import IZlibTransport

logger = logging.getLogger(__name__)


class IMpi:
	MPI.pickle.__init__(cloudpickle.dumps, cloudpickle.loads)

	def __init__(self, propertyParser, partition_tools, context):
		self.__propertyParser = propertyParser
		self.__partition_tools = partition_tools
		self.__context = context

	def gather(self, part, root):
		if self.executors() == 1: return
		self.__gatherImpl(self.native(), part, root, True)

	def bcast(self, part, root):
		if self.executors() == 1: return
		if part.type() == IMemoryPartition.TYPE:
			if part._IMemoryPartition__cls == bytearray:
				sz = self.native().bcast(part.size(), root)
				if not self.isRoot(root):
					part._IMemoryPartition__elements = bytearray(sz)

				with memoryview(part._IMemoryPartition__elements) as men:
					self.native().Bcast((men, men.nbytes, MPI.BYTE), root)

			elif part._IMemoryPartition__cls.__name__ == 'INumpyWrapper':
				elems = part._IMemoryPartition__elements
				sz, dtype = self.native().bcast((part.size(), elems.array.dtype), root)
				if not self.isRoot(root):
					tmp = self.__partition_tools.newNumpyMemoryPartition(dtype, sz)
					self.__partition_tools.swap(part, tmp)
					elems = part._IMemoryPartition__elements
					elems._resize(sz)

				self.native().Bcast((elems.array, elems.bytes(), MPI.BYTE), root)
			else:
				buffer = IMemoryBuffer(part.bytes())
				if self.isRoot(root):
					native = self.__propertyParser.nativeSerialization()
					part.write(buffer, self.__propertyParser.msgCompression(), native, listHeader=False)
				sz = self.native().bcast(buffer.writeEnd(), root)
				if not self.isRoot(root):
					buffer.setBufferSize(sz)

				self.native().Bcast((buffer.getBuffer().address(), sz, MPI.BYTE), root)
				if not self.isRoot(root):
					buffer.setWriteBuffer(sz)
					part.clear()
					part.read(buffer)

		elif part.type() == IRawMemoryPartition.TYPE:
			part.sync()
			buffer = part._transport
			elems, sz, header = self.native().bcast((part.size(),
			                                         buffer.writeEnd(),
			                                         part._header),
			                                        root)
			if not self.isRoot(root):
				part._elements = elems
				part._header = header
				buffer.setBufferSize(sz)
				buffer.setWriteBuffer(sz)
			self.native().Bcast((buffer.getBuffer().address(), sz, MPI.BYTE), root)

		else:  # IDiskPartition.TYPE
			part.sync()
			path = self.native().bcast(part.getPath(), root)

			if not self.isRoot(root):
				part._IDiskPartition__destroy = True
				rcv = self.__partition_tools.copyDiskPartition(path)
				self.__partition_tools.swap(part, rcv)

	def driverGather(self, group: MPI.Intracomm, part_group):
		driver = group.Get_rank() == 0
		exec0 = group.Get_rank() == 1
		max_partition = c_int(0)
		protocol = c_byte()
		same_protocol = c_bool()
		storage = bytearray()
		storage_length = c_int(0)

		if driver:
			protocol = c_byte(IObjectProtocol.PYTHON_PROTOCOL)
		else:
			max_partition = c_int(part_group.partitions())
			if part_group:
				storage = bytearray((part_group[0].type() + ";").encode("utf-8"))
			storage_length = c_int(len(storage))

		group.Allreduce(MPI.IN_PLACE, (max_partition, MPI.INT), MPI.MAX)
		group.Bcast((protocol, 1, MPI.BYTE), 0)
		if max_partition.value == 0:
			return
		group.Allreduce(MPI.IN_PLACE, (storage_length, MPI.INT), MPI.MAX)
		max_partition = max_partition.value
		storage_length = storage_length.value
		storage.extend(bytes(storage_length - len(storage)))
		storage_v = bytearray(storage_length * group.Get_size()) if driver else bytearray()
		with memoryview(storage) as c_storage:
			with memoryview(storage_v) as c_storage_v:
				group.Gather((c_storage, storage_length, MPI.BYTE), (c_storage_v, storage_length, MPI.BYTE), 0)

		for i in range(0, len(storage_v), storage_length):
			if storage_v[i] != 0:
				storage = storage_v[i:i + storage_length]
				break

		with memoryview(storage) as c_storage:
			group.Bcast((c_storage, storage_length, MPI.BYTE), 0)

		storage = storage[:-1].decode("utf-8")

		if driver:
			group.Recv((same_protocol, 1, MPI.BOOL), 1, 0)
		else:
			same_protocol = c_bool(protocol.value == IObjectProtocol.PYTHON_PROTOCOL)
			if exec0:
				group.Send((same_protocol, 1, MPI.BOOL), 0, 0)

		logger.info("Comm: driverGather storage: " + str(storage) + ", operations: " + str(max_partition))
		if storage != IMemoryPartition.TYPE and not same_protocol.value:
			if not driver and len(part_group) > 0 and part_group[0]._native:
				for i in range(len(part_group)):
					new_p = self.__partition_tools.newPartition(storage)
					new_p._native = False
					part_group[i].copyTo(new_p)
					part_group[i] = new_p

		part_sp = None
		for i in range(0, max_partition):
			if i < len(part_group):
				part_sp = part_group[i]
			elif part_sp is None or not part_sp.empty():
				part_sp = self.__partition_tools.newPartition(storage)
				if driver:
					part_group.add(part_sp)
			self.__gatherImpl(group, part_sp, 0, same_protocol.value)

	def driverGather0(self, group: MPI.Intracomm, part_group):
		rank = group.Get_rank()
		sub_group = group.Split(1 if rank < 2 else 0, rank)
		if rank < 2:
			self.driverGather(sub_group, part_group)
		sub_group.Free()

	def driverScatter(self, group: MPI.Intracomm, part_group, partitions):
		id = group.Get_rank()
		driver = id == 0
		exec0 = id == 1
		execs = group.Get_size() - 1
		same_protocol = c_bool()
		sz = 0
		partsv = list()
		szv = list()
		displs = list()
		cls = None

		protocol = c_byte(IObjectProtocol.PYTHON_PROTOCOL)
		group.Bcast((protocol, 1, MPI.BYTE), 0)

		if driver:
			src = part_group[0]
			sz = len(src)
			group.Recv((same_protocol, 1, MPI.BOOL), 1, 0)
		else:
			same_protocol = c_bool(protocol.value == IObjectProtocol.PYTHON_PROTOCOL)
			if exec0:
				group.Send((same_protocol, 1, MPI.BOOL), 0, 0)

		if same_protocol:
			cls = group.bcast(src._IMemoryPartition__cls if driver else None, 0)
			self.__context.vars()['STORAGE_CLASS'] = cls
			if cls.__name__ == 'INumpyWrapper':
				dtype = group.bcast(src._IMemoryPartition__elements.array.dtype if driver else None, 0)
				self.__context.vars()['STORAGE_CLASS_DTYPE'] = dtype

		logger.info("Comm: driverScatter partitions: " + str(partitions))

		execs_parts = int(partitions / execs)
		if partitions % execs:
			execs_parts += 1

		same_protocol = same_protocol.value
		if driver:
			execs_elems = int(sz / partitions)
			remainder = sz % partitions
			partsv.extend([0] * execs_parts)
			partsv.extend([execs_parts + 1] * remainder)
			partsv.extend([execs_elems] * (partitions - remainder))
			partsv.extend([0] * (len(partsv) - execs_parts * (execs + 1)))

			szv.append(0)
			displs.append(0)
			offset = 0

			if same_protocol and cls.__name__ in ('bytearray', 'INumpyWrapper'):
				src = src._IMemoryPartition__elements
				itemsize = 1
				if cls.__name__ == 'INumpyWrapper':
					itemsize = src.itemsize()
					src = src.array

				for i in range(execs_parts, len(partsv)):
					partsv[i] *= itemsize
					offset += partsv[i]
					if (i + 1) % execs_parts == 0:
						displs.append(szv[-1] + displs[-1])
						szv.append(offset - displs[-1])

			else:
				elems = src._IMemoryPartition__elements
				native = self.__propertyParser.nativeSerialization() and same_protocol
				cmp = self.__propertyParser.msgCompression()
				buffer = IMemoryBuffer()
				zlib = IZlibTransport(buffer, cmp)
				proto = IObjectProtocol(zlib)
				wrote = 0
				for i in range(execs_parts, len(partsv)):
					proto.writeObject(elems[offset:offset + partsv[i]], native)
					offset += partsv[i]
					zlib.flush()
					zlib.reset()
					partsv[i] = buffer.writeEnd() - wrote
					wrote += partsv[i]
					if (i + 1) % execs_parts == 0:
						displs.append(szv[-1] + displs[-1])
						szv.append(buffer.writeEnd() - displs[-1])
				src = buffer.getBuffer().address()
			partsv = (c_int * len(partsv))(*partsv)
		else:
			partsv = (c_int * execs_parts)()

		group.Scatter((partsv, execs_parts, MPI.INT), MPI.IN_PLACE if driver else (partsv, execs_parts, MPI.INT), 0)
		if not driver:
			for lsz in partsv:
				sz += lsz
			buffer = IMemoryBuffer(sz)
			buffer.setWriteBuffer(sz)
			src = buffer.getBuffer().address()

		with memoryview(src) as c_src:
			group.Scatterv((c_src, szv, MPI.BYTE), MPI.IN_PLACE if driver else (c_src, sz, MPI.BYTE), 0)

		if not driver:
			offset = 0
			if same_protocol and cls == bytearray:
				for lsz in partsv:
					if lsz == 0:
						break
					part = IMemoryPartition(lsz, cls=cls)
					part._IMemoryPartition__elements = bytearray(lsz)
					part._IMemoryPartition__elements[0:lsz] = buffer.getBuffer()[offset:offset + lsz]
					offset += lsz
					part_group.add(part)

			elif same_protocol and cls.__name__ == 'INumpyWrapper':
				for lsz in partsv:
					if lsz == 0:
						break
					part = IMemoryPartition(lsz, cls=cls)
					elems = part._IMemoryPartition__elements
					elems._resize(int(lsz / elems.itemsize()))
					memmove(elems.array.ctypes.data, buffer.getBuffer().address(offset), lsz)
					offset += lsz
					part_group.add(part)
			else:
				for lsz in partsv:
					if lsz == 0:
						break
					view = IMemoryBuffer(buf=buffer.getBuffer().offset(offset), sz=lsz)
					view.setReadBuffer(0)
					part = self.__partition_tools.newPartition()
					part.read(view)
					offset += lsz
					part_group.add(part)

	class MsgOpt:

		def __init__(self, same_protocol, same_storage):
			self.same_protocol = same_protocol
			self.same_storage = same_storage

	def getMsgOpt(self, group, ptype, send, other, tag):
		id = group.Get_rank()
		source = id if send else other
		dest = other if send else id
		same_protocol = c_bool()
		same_storage = c_bool()

		if id == source:
			protocol = c_byte(IObjectProtocol.PYTHON_PROTOCOL)
			storage = bytearray(ptype.encode("utf-8"))
			length = c_int(len(storage))
			group.Send((protocol, 1, MPI.BYTE), dest, tag)
			group.Recv((same_protocol, 1, MPI.BOOL), dest, tag)
			group.Send((length, 1, MPI.INT), dest, tag)
			with memoryview(storage) as c_storage:
				group.Send((c_storage, length.value, MPI.BYTE), dest, tag)
			group.Recv((same_storage, 1, MPI.BOOL), dest, tag)
		else:
			protocol = c_byte()
			length = c_int()
			group.Recv((protocol, 1, MPI.BYTE), source, tag)
			same_protocol = c_bool(protocol.value == IObjectProtocol.PYTHON_PROTOCOL)
			group.Send((same_protocol, 1, MPI.BOOL), source, tag)
			group.Recv((length, 1, MPI.INT), source, tag)
			storage = bytearray(length.value)
			with memoryview(storage) as c_storage:
				group.Recv((c_storage, length.value, MPI.BYTE), source, tag)
			storage = storage.decode("utf-8")
			same_storage = c_bool(ptype == storage)
			group.Send((same_storage, 1, MPI.BOOL), source, tag)

		return self.MsgOpt(same_protocol, same_storage)

	def sendGroup(self, group: MPI.Intracomm, part, dest, tag, opt=None):
		if opt is None:
			opt = self.getMsgOpt(group, part.type(), True, dest, tag)
		self.__sendRecvGroup(group, part, group.Get_rank(), dest, tag, opt)

	def send(self, part, dest, tag):
		self.__sendRecv(part, self.rank(), dest, tag)

	def recvGroup(self, group: MPI.Intracomm, part, source, tag, opt=None):
		if opt is None:
			opt = self.getMsgOpt(group, part.type(), False, source, tag)
		self.__sendRecvGroup(group, part, source, group.Get_rank(), tag, opt)

	def recv(self, part, source, tag):
		self.__sendRecv(part, source, self.rank(), tag)

	def sendRcv(self, sendp, rcvp, other, tag):
		# possible optimization, implement as sendRcv so that both processes serialize and deserialize at the same time.
		if self.rank() > other:
			self.send(sendp, other, tag)
			self.recv(rcvp, other, tag)
		else:
			self.recv(rcvp, other, tag)
			self.send(sendp, other, tag)

	def barrier(self):
		self.native().Barrier()

	def isRoot(self, root):
		return self.rank() == root

	def rank(self):
		return self.__context.executorId()

	def executors(self):
		return self.__context.executors()

	def native(self) -> MPI.Intracomm:
		return self.__context.mpiGroup()

	def __displs(self, szv):
		l = [0]
		v = l[0]
		for sz in szv:
			v += sz
			l.append(v)
		return l

	def __displs2(self, szv):
		l = [(0, 0)]
		v = l[0]
		for sz, sz_bytes in szv:
			v = v[0] + sz, v[1] + sz_bytes
			l.append(v)
		return l

	def __getList(self, szv, c):
		return list(map(lambda v: v[c], szv))

	def __gatherImpl(self, group: MPI.Intracomm, part, root, same_protocol):
		rank = group.Get_rank()
		executors = group.Get_size()
		if part.type() == IMemoryPartition.TYPE:
			cls = None
			if same_protocol:
				cls = part._IMemoryPartition__cls.__name__ if part and rank != root else None
				cls_list = group.gather(cls, root)
				if rank == root:
					for cls in cls_list:
						if cls is not None:
							break
				cls = group.bcast(cls, root)

			if cls == 'bytearray' and same_protocol:
				sz = part.size()
				szv = (c_int * executors)() if rank == root else None
				group.Gather((c_int(sz), 1, MPI.INT), (szv, 1, MPI.INT), root)

				if root == rank:
					displs = self.__displs(szv)
					if root > 0:
						part._IMemoryPartition__elements = part._IMemoryPartition__elements.zfill(displs[root + 1])
					part._IMemoryPartition__elements.extend(bytes(displs[-1] - part.size()))
					with memoryview(part._IMemoryPartition__elements) as men:
						group.Gatherv(MPI.IN_PLACE, (men, szv, MPI.BYTE), root)
				else:
					with memoryview(part._IMemoryPartition__elements) as men:
						group.Gatherv((men, sz, MPI.BYTE), None, root)
			elif cls == 'INumpyWrapper' and same_protocol:
				elems = part._IMemoryPartition__elements
				sz = part.size() * elems.itemsize()
				szdtv = group.gather((sz, elems.array.dtype), root)

				if root == rank:
					szv = self.__getList(szdtv, 0)
					displs = self.__displs(szv)

					if part.empty():
						for i in range(len(szdtv)):
							if szv[i] > 0:
								tmp = self.__partition_tools.newNumpyMemoryPartition(szdtv[i][1],
								                                                     int(displs[-1] / elems.itemsize()))
								self.__partition_tools.swap(part, tmp)
								break
						elems = part._IMemoryPartition__elements

					elems._resize(int(displs[-1] / elems.itemsize()))
					if root > 0:
						n = int(sz / elems.itemsize())
						padd = int(displs[root] / elems.itemsize())
						elems[padd:padd + n] = elems[0:n]
					group.Gatherv(MPI.IN_PLACE, (elems.array, szv, MPI.BYTE), root)
				else:
					group.Gatherv((elems.array, sz, MPI.BYTE), None, root)
			else:
				if root != rank:
					buffer = IMemoryBuffer(part.bytes())
					native = self.__propertyParser.nativeSerialization() and same_protocol
					part.write(buffer, self.__propertyParser.msgCompression(), native, listHeader=False)
					sz = buffer.writeEnd()
					buffer.resetBuffer()
				else:
					buffer = IMemoryBuffer(1)
					sz = 0
				szv = (c_int * executors)() if rank == root else None
				group.Gather((c_int(sz), 1, MPI.INT), (szv, 1, MPI.INT), root)
				if root == rank:
					displs = self.__displs(szv)
					buffer.setBufferSize(displs[-1])
					group.Gatherv(MPI.IN_PLACE, (buffer.getBuffer().address(), szv, MPI.BYTE), root)
					rcv = IMemoryPartition(part._IMemoryPartition__native, part._IMemoryPartition__cls)
					for i in range(self.executors()):
						if i != root:
							buffer.setWriteBuffer(displs[i + 1])
							rcv.read(buffer)
						else:
							# Avoid serialization own elements
							part.moveTo(rcv)
					self.__partition_tools.swap(part, rcv)

				else:
					group.Gatherv((buffer.getBuffer().address(), sz, MPI.BYTE), None, root)
		elif part.type() == IRawMemoryPartition.TYPE:
			ELEMS = 0
			BYTES = 1
			part.sync()
			buffer = part._transport
			tmp_buffer = IMemoryBuffer()
			hsz = (c_int * executors)() if rank == root else None
			group.Gather((c_int(part._header_size), 1, MPI.INT), (hsz, 1, MPI.INT), root)
			group.Gather((buffer.getBuffer().address(), part._HEADER, MPI.BYTE),
			             (tmp_buffer.getBuffer().address(), part._HEADER, MPI.BYTE), root)
			sz = part.size()
			sz_bytes = buffer.writeEnd() - part._HEADER
			szv = (c_int * 2 * executors)() if rank == root else None
			group.Gather(((c_int * 2)(sz, sz_bytes), 2, MPI.INT), (szv, 2, MPI.INT), root)

			if root == rank:
				displs = self.__displs2(szv)
				part._elements = displs[-1][ELEMS]
				buffer.setBufferSize(displs[-1][BYTES] + part._HEADER)
				buffer.setWriteBuffer(buffer.getBufferSize())
				if root > 0:
					buffer.getBuffer().padding(displs[root][BYTES], sz_bytes + part._HEADER)
				group.Gatherv(MPI.IN_PLACE,
				              (buffer.getBuffer().address(part._HEADER), self.__getList(szv, BYTES), MPI.BYTE),
				              root)
				for i in range(len(szv)):
					if szv[i][ELEMS] > 0:
						buffer.getBuffer()[0:part._HEADER] = tmp_buffer.getBuffer()[
						                                     part._HEADER * i:part._HEADER * (i + 1)]
						part._header_size = hsz[i]
						part._elements -= szv[i][ELEMS]
						part._readHeader(IZlibTransport(part._readTransport()))
						break

			else:
				group.Gatherv((buffer.getBuffer().address(part._HEADER), sz_bytes, MPI.BYTE), None, root)
		else:  # IDiskPartition.TYPE
			part.sync()
			path = part.getPath().encode("utf-8")
			sz = len(path)
			szv = (c_int * executors)() if rank == root else None
			displs = None
			full_path = bytearray()
			group.Gather((c_int(sz), 1, MPI.INT), (szv, 1, MPI.INT), root)
			if rank == root:
				displs = self.__displs(szv)
				full_path = bytearray(displs[-1])

			with memoryview(path) as c_path:
				with memoryview(full_path) as c_full_path:
					group.Gatherv((c_path, sz, MPI.BYTE), (c_full_path, szv, MPI.BYTE), root)

			if root == rank:
				path = part.getPath()
				part.rename(path + ".tmp")
				rcv = IDiskPartition(path, part._compression, part._native, persist=True, read=False)
				rcv._IDiskPartition__destroy = part._IDiskPartition__destroy
				part._IDiskPartition__destroy = True
				for i in range(executors):
					if i != rank:
						src = full_path[displs[i]:displs[i] + szv[i]].decode("utf-8")
						IDiskPartition(src, part._compression, part._native, persist=True, read=True).copyTo(rcv)
					else:
						part.moveTo(rcv)
				self.__partition_tools.swap(part, rcv)
			group.Barrier()

	def __sendRecv(self, part, source, dest, tag):
		self.__sendRecvImpl(self.native(), part, source, dest, tag, True)

	def __sendRecvGroup(self, group: MPI.Intracomm, part, source, dest, tag, opt):
		id = group.Get_rank()
		same_protocol = opt.same_protocol
		same_storage = opt.same_storage

		if same_storage.value:
			self.__sendRecvImpl(group, part, source, dest, tag, same_protocol)
		else:
			buffer = IMemoryBuffer(part.bytes())
			if id == source:
				part.write(buffer, self.__propertyParser.msgCompression())
				sz = buffer.writeEnd()
				buffer.resetBuffer()
				group.Send((c_int(sz), 1, MPI.INT), dest, tag)
				group.Send((buffer.getBuffer().address(), sz, MPI.BYTE), dest, tag)
			else:
				sz = c_int()
				group.Recv((sz, 1, MPI.INT), source, tag)
				buffer.setBufferSize(sz.value)
				group.Recv((buffer.getBuffer().address(), sz.value, MPI.BYTE), source, tag)
				buffer.setWriteBuffer(sz.value)
				part.read(buffer)

	def __sendRecvImpl(self, group: MPI.Intracomm, part, source, dest, tag, same_protocol):
		id = group.Get_rank()
		if part.type() == IMemoryPartition.TYPE:
			if same_protocol:
				if id == source:
					group.send(part._IMemoryPartition__cls, dest, tag)
				else:
					cls = group.recv(None, source, tag)
					if cls.__name__ != part._IMemoryPartition__cls.__name__:
						tmp = self.__partition_tools.newMemoryPartition(part.size(), cls)
						self.__partition_tools.swap(tmp, part)

			if part._IMemoryPartition__cls == bytearray and same_protocol:
				sz = part.size()
				if id == source:
					group.send(sz, dest, tag)
					with memoryview(part._IMemoryPartition__elements) as men:
						group.Send((men, sz, MPI.BYTE), dest, tag)
				else:
					sz = group.recv(None, source, tag)
					buff = bytearray(sz)
					with memoryview(buff) as men:
						group.Recv((men, sz, MPI.BYTE), source, tag)
					part._IMemoryPartition__elements.extend(buff)

			elif part._IMemoryPartition__cls.__name__ == 'INumpyWrapper' and same_protocol:
				elems = part._IMemoryPartition__elements
				sz = part.size() * elems.itemsize()
				if id == source:
					group.send((sz, elems.array.dtype), dest, tag)
					elems = part._IMemoryPartition__elements
					group.Send((elems.array, sz, MPI.BYTE), dest, tag)
				else:
					init = len(elems)
					sz, dtype = group.recv(None, source, tag)
					if part.empty():
						tmp = self.__partition_tools.newNumpyMemoryPartition(dtype, sz)
						elems = tmp._IMemoryPartition__elements
						elems._resize(int(sz / elems.itemsize()))
						group.Recv((elems.array[init:], sz, MPI.BYTE), source, tag)
						self.__partition_tools.swap(part, tmp)
					else:
						elems = part._IMemoryPartition__elements
						elems._resize(init + int(sz / elems.itemsize()))
						group.Recv((elems.array[init:], sz, MPI.BYTE), source, tag)
			else:
				buffer = IMemoryBuffer(part.bytes())
				if id == source:
					native = self.__propertyParser.nativeSerialization() and same_protocol
					part.write(buffer, self.__propertyParser.msgCompression(), native, listHeader=False)
					sz = buffer.writeEnd()
					buffer.resetBuffer()
					group.Send((c_int(sz), 1, MPI.INT), dest, tag)
					group.Send((buffer.getBuffer().address(), sz, MPI.BYTE), dest, tag)
				else:
					sz = c_int()
					group.Recv((sz, 1, MPI.INT), source, tag)
					buffer.setBufferSize(sz.value)
					group.Recv((buffer.getBuffer().address(), sz.value, MPI.BYTE), source, tag)
					buffer.setWriteBuffer(sz.value)
					part.read(buffer)
		elif part.type() == IRawMemoryPartition.TYPE:
			ELEMS = 0
			BYTES = 1
			part.sync()
			if id == source:
				hsz = c_int(part._header_size)
				group.Send((hsz, 1, MPI.INT), dest, tag)
			else:
				hsz = c_int()
				group.Recv((hsz, 1, MPI.INT), source, tag)

			buffer = part._transport
			sz = (c_int * 2)(part.size(), buffer.writeEnd())
			if id == source:
				group.Send((sz, 2, MPI.INT), dest, tag)
				group.Send((buffer.getBuffer().address(), sz[BYTES], MPI.BYTE), dest, tag)
			else:
				group.Recv((sz, 2, MPI.INT), source, tag)
				if part:
					tmp = self.__partition_tools.newRawMemoryPartition()
					buffer = tmp._transport
				else:
					tmp = part
				tmp._header_size = hsz.value
				buffer.setBufferSize(sz[BYTES])
				buffer.setWriteBuffer(buffer.getBufferSize())
				group.Recv((buffer.getBuffer().address(), sz[BYTES], MPI.BYTE), source, tag)
				tmp._readHeader(IZlibTransport(tmp._readTransport()))
				if part != tmp:
					tmp.moveTo(part)
		else:
			part.sync()
			path = part.getPath().encode("utf-8")
			sz = c_int(len(path))

			if id == source:
				group.Send((sz, 1, MPI.INT), dest, tag)
				with memoryview(path) as c_path:
					group.Send((c_path, sz.value, MPI.BYTE), dest, tag)
				group.Recv((sz, 1, MPI.INT), dest, tag)
			else:
				group.Recv((sz, 1, MPI.INT), source, tag)
				path = bytearray(sz.value)
				with memoryview(path) as c_path:
					group.Recv((c_path, sz.value, MPI.BYTE), source, tag)
				path = path.decode("utf-8")
				rcv = self.__partition_tools.copyDiskPartition(path)
				group.Send((sz, 1, MPI.INT), source, tag)
				if part.size() == 0:
					part.persist(False)
					self.__partition_tools.swap(part, rcv)
				else:
					rcv.copyTo(part)

	def exchange_sync(self, input, output):
		executors = self.executors()
		if executors == 1 or len(input) < 2:
			for part in input:
				part.fit()
				output.add(part)
			return

		numPartitions = input.partitions()
		block = int(numPartitions / executors)
		remainder = numPartitions % executors
		parts_targets = [None] * (block + 1) * executors

		p = 0
		for i in range(executors):
			for j in range(block):
				parts_targets[j * executors + i] = (p + j, i)
			p += block
			if i < remainder:
				parts_targets[block * executors + i] = (p, i)
				p += 1
		parts_targets = list(filter(None, parts_targets))

		cores = self.__propertyParser.cores()
		p_type = input[0].type()
		op_type = p_type
		cls = None
		comm = self.native()
		wins = list()
		onlyShared = False
		if self.__propertyParser.transportCores() > 0 and cores > 1 and p_type != IDiskPartition.TYPE:
			logger.info("Local exchange init")

			class IMpiBuffer(IBuffer):

				def __init__(self, sz, address):
					super().__init__(sz, address)

				def realloc(self, sz):  # Gather will call it
					aux = IBuffer(sz)
					aux[0:len(self)] = self[0:len(self)]
					aux.__dict__, self.__dict__ = self.__dict__, aux.__dict__

				def free(self):
					pass

			def send(part, target):
				part.sync()
				trans = part._transport
				info = shared_comm.allgather((trans.writeEnd() - IRawMemoryPartition._HEADER, len(part),
				                              (part._header, part._type) if len(part) > 0 else None))
				if sum(map(lambda a: a[1], info)) == info[target][1]:
					if rank == target:
						return part
					return None
				lsz = info[rank][0]
				sz = sum(map(lambda a: a[0], info)) + IRawMemoryPartition._HEADER
				disp = sum(list(map(lambda a: a[0], info))[0:rank]) + IRawMemoryPartition._HEADER
				elems = sum(map(lambda a: a[1], info))
				win = MPI.Win.Allocate_shared(sz if rank == target else 0, 1, comm=shared_comm)
				wins.append(win)
				buf, itemsize = win.Shared_query(target)
				buffer = IMpiBuffer(sz, buf.address)

				buffer[disp:disp + lsz] = trans.getBuffer()[
				                          IRawMemoryPartition._HEADER: IRawMemoryPartition._HEADER + lsz]
				if rank != target and len(part) == 0:
					buffer[0:IRawMemoryPartition._HEADER] = trans.getBuffer()[0:IRawMemoryPartition._HEADER]
				shared_comm.Barrier()
				if rank == target:
					part._transport = IMemoryBuffer(sz, buffer, IMemoryBuffer.MemoryPolicy.TAKE_OWNERSHIP)
					part._zlib = IZlibTransport(part._transport, part._compression)
					part._transport.setReadBuffer(0)
					if len(part) == 0:
						header_type = list(filter(None, map(lambda a: a[2], info)))[0]
						part._header = header_type[0]
						part._type = header_type[1]
					part._elements = elems
					part._writeHeader()
					if onlyShared:  # call it if no Gather
						buffer.realloc(len(buffer))
					return part
				return None

			if p_type == IMemoryPartition.TYPE:
				cls = input[0]._IMemoryPartition__cls
				if cls.__name__ != 'INumpyWrapper':
					p_type = IRawMemoryPartition.TYPE
					for i in range(len(input)):
						aux = self.__partition_tools.newRawMemoryPartition(input[i].bytes(), compression=0)
						input[i].moveTo(aux)
						input[i] = aux

				else:
					def send2(part, target):
						array = part._IMemoryPartition__elements
						info = shared_comm.allgather(len(array))
						total = sum(info)
						if info[target] == total:
							if rank == target:
								return part
							return None
						disp = sum(info[0:rank])
						win = MPI.Win.Allocate_shared(total * array.itemsize() if rank == target else 0, 1,
						                              comm=shared_comm)
						wins.append(win)
						buf, itemsize = win.Shared_query(target)
						from ignis.executor.core.io.INumpy import INumpyWrapper as INumpyWrapperOld
						import numpy
						class INumpyWrapper(INumpyWrapperOld):
							def __init__(self, array):
								super().__init__(array=array)

							def _resize(self, n):  # Gather will call it
								aux = INumpyWrapperOld(n, self.array.dtype)
								aux[0:len(self)] = self[0:len(self)]
								del self.array
								self.array = aux

						wrap = numpy.ndarray(buffer=buf, dtype=array.array.dtype, shape=(total,))
						wrap[disp:disp + info[rank]] = array.array[0:info[rank]]

						shared_comm.Barrier()
						if rank == target:
							aux = INumpyWrapper(wrap)
							part._IMemoryPartition__elements = aux
							if onlyShared:  # call it if no Gather
								aux._resize(len(aux))

							return part
						return None

					send = send2

			comm = self.native()
			shared_comm = comm.Split_type(MPI.COMM_TYPE_SHARED)
			onlyShared = shared_comm.Get_size() == self.executors()
			rank = shared_comm.Get_rank()
			cores = shared_comm.Get_size()
			if shared_comm.Get_size() == 1:
				logger.info("Local exchange aborting, no shared memory enabled")
			else:
				for i in range(numPartitions):
					p = parts_targets[i][0]
					target = parts_targets[i][1] % cores
					input[p] = send(input[p], target)
					if target == rank:
						aux = self.__partition_tools.newMemoryPartition(len(input[p]), cls)
						input[p].moveTo(aux)
						input[p] = aux

			if not onlyShared:
				logger.info("Global exchange init")
				comm = comm.Split(rank % cores)

		if not onlyShared:
			for i in range(numPartitions):
				p = parts_targets[i][0]
				if input[p] is None or comm.Get_size() == 1:
					continue
				target = parts_targets[i][1]
				self.__gatherImpl(comm, input[p], target, True)
				if self.isRoot(target):
					input[p].fit()
				else:
					input[p] = None

			if comm != self.native():
				comm.Free()

		if wins:
			for win in wins:
				win.Free()
			shared_comm.Free()

		for i in range(numPartitions):
			if input[i] is not None:
				if p_type == op_type:
					output.add(input[i])
				else:
					aux = self.__partition_tools.newMemoryPartition(len(input[i]), cls)
					input[i].moveTo(aux)
					input[i] = None
					output.add(aux)

		input.clear()

	def exchange_async(self, input, output):
		executors = self.executors()
		rank = self.rank()
		numPartitions = len(input)
		block = int(numPartitions / executors)
		remainder = numPartitions % executors
		ranges = list()
		queue = list()

		for i in range(executors):
			if i < remainder:
				init = (block + 1) * i
				end = init + block + 1
			else:
				init = (block + 1) * remainder + block * (i - remainder)
				end = init + block
			ranges.append((init, end))

		m = executors if executors % 2 == 0 else executors + 1
		id = 0
		id2 = m * m - 2
		for i in range(m - 1):
			if rank == id % (m - 1):
				queue.append(m - 1)
			if rank == m - 1:
				queue.append(id % (m - 1))
			id += 1
			for j in range(1, int(m / 2)):
				if rank == id % (m - 1):
					queue.append(id2 % (m - 1))
				if rank == id2 % (m - 1):
					queue.append(id % (m - 1))
				id += 1
				id2 -= 1

		ignores = [False] * len(queue)
		for i in range(len(queue)):
			other = queue[i]
			ignore = True
			if other == executors:
				continue
			for j in range(ranges[other][0], ranges[other][1]):
				ignore = ignore and input[j].empty()
			ignore_other = self.native().sendrecv(ignore, source=other, dest=other)
			if ignore and ignore_other:
				ignores[i] = True
				for j in range(ranges[other][0], ranges[other][1]):
					input[j] = None

		for i in range(len(queue)):
			other = queue[i]
			if ignores[i] or other == executors:
				continue
			other_part = ranges[other][0]
			other_end = ranges[other][1]
			me_part = ranges[rank][0]
			me_end = ranges[rank][1]
			its = max(other_end - other_part, me_end - me_part)

			for j in range(its):
				if other_part >= other_end or me_part >= me_end:
					if other_part >= other_end:
						self.recv(input[me_part], other, 0)
					elif me_part >= me_end:
						self.send(input[other_part], other, 0)
					else:
						continue
				else:
					self.sendRcv(input[other_part], input[me_part], other, 0)
				input[other_part] = None
				other_part += 1
				me_part += 1

		for i in range(numPartitions):
			if input[i]:
				input[i].fit()
				output.add(input[i])
		input.clear()
