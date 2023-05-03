import logging
import random

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl, MPI

logger = logging.getLogger(__name__)


class IRepartitionImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data, logger)

	def repartition(self, numPartitions, preserveOrdering, global_):
		if not global_ or self._executor_data.mpi().executors() == 1:
			self.__local_repartition(numPartitions)
		elif preserveOrdering:
			self.__ordered_repartition(numPartitions)
		else:
			self.__unordered_repartition(numPartitions)

	def __ordered_repartition(self, numPartitions):
		input = self._executor_data.getAndDeletePartitions()
		executors = self._executor_data.getContext().executors()
		rank = self._executor_data.mpi().rank()
		local_offset = list()
		local_count = 0
		global_count = 0
		global_offset = 0
		local_offset.append(0)
		for part in input:
			local_count += len(part)
			local_offset.append(len(part))
		executors_count = self._executor_data.mpi().native().allgather(local_count)
		for i in range(executors):
			if i < rank:
				global_offset += executors_count[i]
			global_count += executors_count[i]
		new_partitions_count = list()
		new_partitions_offset = list()
		new_partitions_first = -1

		block = int(global_count / numPartitions)
		remainder = global_count % numPartitions
		new_partitions_offset.append(0)
		for i in range(numPartitions):
			new_partitions_count.append(block)
			if i < remainder:
				new_partitions_count[i] += 1
			new_partitions_offset.append(new_partitions_offset[-1] + new_partitions_count[i])
			if global_offset <= new_partitions_offset[i + 1] and new_partitions_first == -1:
				new_partitions_first = i

		logger.info("Repartition: ordered repartition from " + str(len(input)) + " partitions")
		global_group = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)

		if len(input) > 0:
			i = new_partitions_first
			required = new_partitions_offset[i + 1] - global_offset
			for p in range(len(input)):
				reader = input[p].readIterator()
				avail = len(input[p])
				while avail > 0:
					if required == 0:
						i += 1
						required = new_partitions_count[i]
					writer = global_group[i].writeIterator()
					its = min(avail, required)
					for _ in range(its):
						writer.write((rank, reader.next()))
					avail -= its
					required -= its
				input[p] = None


		logger.info("Repartition: exchanging new partitions")
		tmp = self._executor_data.getPartitionTools().newPartitionGroup()
		self.exchange(global_group, tmp)
		output = self._executor_data.getPartitionTools().newPartitionGroup()

		for p in range(len(tmp)):
			part = tmp[p]
			if part.empty():
				continue
			new_part = self._executor_data.getPartitionTools().newPartition()
			writer = new_part.writeIterator()
			men_part = self._executor_data.getPartitionTools().newMemoryPartition()
			part.moveTo(men_part)
			first = [0] * executors
			count = [0] * executors
			for i in reversed(range(len(men_part))):
				first[men_part[i][0]] = i
				count[men_part[i][0]] += 1
			for e in range(executors):
				for i in range(first[e], first[e] + count[e]):
					writer.write(men_part[i][1])
			tmp[p] = None
			output.add(new_part)

		self._executor_data.setPartitions(output)

	def __unordered_repartition(self, numPartitions):
		input = self._executor_data.getAndDeletePartitions()
		executors = self._executor_data.getContext().executors()
		rank = self._executor_data.mpi().rank()
		local_count = 0
		global_count = 0
		msg_max = 0
		msg_num = 0
		for part in input:
			local_count += len(part)
			if len(part) > msg_max:
				msg_max = len(part)
		logger.info("Repartition: unordered repartition from " + str(len(input)) + " partitions")
		executor_count = self._executor_data.mpi().native().allgather(local_count)
		msg_max = self._executor_data.mpi().native().allreduce(msg_max, MPI.MAX)

		global_count = sum(executor_count)
		average = int(global_count / executors)
		executors_overhead = list()
		for i in range(executors):
			executors_overhead.append(executor_count[i] - average)

		src_target_size = list()
		msg_count = [0 for _ in range(executors)]
		other = 0
		for i in range(executors):
			while executors_overhead[i] > 0 and other < executors:
				if executors_overhead[other] >= 0:
					other += 1
					continue
				if executors_overhead[other] + executors_overhead[i] > 0:
					elems = -executors_overhead[other]
					executors_overhead[other] = 0
					executors_overhead[i] -= elems
				else:
					elems = executors_overhead[i]
					executors_overhead[i] = 0
					executors_overhead[other] += elems

				while elems > 0:
					total = min(elems, msg_max)
					src_target_size.append((i, other, total))
					elems -= total
					msg_count[i] += 1
					if msg_count[i] > msg_num:
						msg_num = msg_count[i]

		shared = self._executor_data.getPartitionTools().newPartitionGroup(msg_num * executors)
		src_part = input[-1]
		reader = src_part.readIterator()
		used = False
		for entry in src_target_size:
			if entry[0] == rank:
				elems = entry[2]
				target = msg_num * entry[1]
				while not shared[target].empty():
					target += 1
				writer = shared[target].writeIterator()

				while elems > 0:
					used = True
					while reader.hasNext() and elems > 0:
						writer.write(reader.next())
						elems -= 1
					if elems > 0:
						input.remove(-1)
						src_part = input[-1]
						reader = src_part.readIterator()

		if used and reader.hasNext():
			new_part = self._executor_data.getPartitionTools().newPartition(src_part.type())
			writer = new_part.writeIterator()
			while reader.hasNext():
				writer.write(reader.next())
			input[-1] = new_part
		logger.info("Repartition: exchanging new partitions")

		tmp = self._executor_data.getPartitionTools().newPartitionGroup()
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		self.exchange(shared, tmp)
		for part in input:
			output.add(part)
		for part in tmp:
			if not part.empty():
				output.add(part)
		self._executor_data.setPartitions(output)
		self.__local_repartition(numPartitions)

	def __local_repartition(self, numPartitions):
		input = self._executor_data.getAndDeletePartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		executors = self._executor_data.getContext().executors()
		elements = 0
		for part in input:
			elements += len(part)
		localPartitions = int(numPartitions / executors)
		if numPartitions % executors > self._executor_data.getContext().executorId():
			localPartitions += 1
		logger.info(
			"Repartition: local repartition from " + str(len(input)) + " to " + str(localPartitions) + " partitions")
		partition_elems = int(elements / localPartitions)
		remainder = elements % localPartitions
		i = 1
		er = len(input[0])
		it = input[0].readIterator()
		for p in range(localPartitions):
			part = self._executor_data.getPartitionTools().newPartition()
			writer = part.writeIterator()
			ew = partition_elems
			if p < remainder:
				ew += 1

			while ew > 0 and (i < len(input) or er > 0):
				if er == 0:
					input[i - 1] = None
					er = len(input[i])
					it = input[i].readIterator()
					i += 1
				while ew > 0 and er > 0:
					writer.write(it.next())
					ew -= 1
					er -= 1
			part.fit()
			output.add(part)

		self._executor_data.setPartitions(output)

	def partitionByRandom(self, numPartitions, seed):
		r = random.Random(seed)
		self.__partitionBy_impl(lambda elem, ctx: r.randint(0, numPartitions), numPartitions)

	def partitionByHash(self, numPartitions):
		self.__partitionBy_impl(lambda elem, ctx: hash(elem), numPartitions)

	def partitionBy(self, f, numPartitions):
		context = self._executor_data.getContext()
		f.before(context)
		call = f.call
		self.__partitionBy_impl(call, numPartitions)
		f.after(context)

	def __partitionBy_impl(self, f, numPartitions):
		input = self._executor_data.getAndDeletePartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		context = self._executor_data.getContext()

		logger.info("Repartition: partitionBy in " + str(len(input)) + " partitions")
		global_group = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
		writers = [part.writeIterator() for part in global_group]
		for p in range(len(input)):
			reader = input[p].readIterator()
			for i in range(len(input[p])):
				elem = reader.next()
				writers[f(elem, context) % numPartitions].write(elem)
				input[p] = None

		logger.info("Repartition: exchanging new partitions")
		self.exchange(global_group, output)
		self._executor_data.setPartitions(output)
