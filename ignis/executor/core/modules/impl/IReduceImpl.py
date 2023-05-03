import logging
import math

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl

logger = logging.getLogger(__name__)


class IReduceImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data, logger)

	def __basicReduce(self, f, result):
		input = self._executor_data.getAndDeletePartitions()
		logger.info("Reduce: reducing " + str(len(input)) + " partitions locally")

		acum = None
		for i in range(len(input)):
			part = input[i]
			if len(part) == 0:
				continue
			if acum:
				acum = self.__aggregatePartition(f, part, acum)
			else:
				acum = self.__reducePartition(f, part)
			input[i] = None
		result.writeIterator().write(acum)

	def reduce(self, f):
		context = self._executor_data.getContext()
		f.before(context)
		elem_part = self._executor_data.getPartitionTools().newMemoryPartition(1)
		self.__basicReduce(f, elem_part)
		self.__finalReduce(f, elem_part)
		f.after(context)

	def treeReduce(self, f):
		context = self._executor_data.getContext()
		f.before(context)
		elem_part = self._executor_data.getPartitionTools().newMemoryPartition(1)
		self.__basicReduce(f, elem_part)
		self.__finalTreeReduce(f, elem_part)
		f.after(context)

	def zero(self, f):
		context = self._executor_data.getContext()
		f.before(context)
		self._executor_data.setVariable("zero", f.call(context))
		f.after(context)

	def aggregate(self, f):
		context = self._executor_data.getContext()
		f.before(context)
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		input = self._executor_data.getAndDeletePartitions()
		partial_reduce = self._executor_data.getPartitionTools().newMemoryPartition(1)
		logger.info("Reduce: aggregating " + str(len(input)) + " partitions locally")

		acum = self._executor_data.getVariable("zero")
		for i in range(len(input)):
			part = input[i]
			if len(part) == 0:
				continue
			acum = self.__aggregatePartition(f, part, acum)
			input[i] = None

		partial_reduce.writeIterator().write(acum)
		output.add(partial_reduce)
		self._executor_data.setPartitions(output)

	def fold(self, f):
		context = self._executor_data.getContext()
		f.before(context)
		input = self._executor_data.getAndDeletePartitions()
		partial_reduce = self._executor_data.getPartitionTools().newMemoryPartition(1)
		logger.info("Reduce: folding " + str(len(input)) + " partitions locally")

		acum = self._executor_data.getVariable("zero")
		for i in range(len(input)):
			part = input[i]
			if len(part) == 0:
				continue
			acum = self.__aggregatePartition(f, part, acum)
			input[i] = None

		partial_reduce.writeIterator().write(acum)
		self.__finalReduce(f, partial_reduce)

	def treeFold(self, f):
		context = self._executor_data.getContext()
		f.before(context)
		input = self._executor_data.getAndDeletePartitions()
		partial_reduce = self._executor_data.getPartitionTools().newMemoryPartition(1)
		logger.info("Reduce: folding " + str(len(input)) + " partitions locally")

		acum = self._executor_data.getVariable("zero")
		for i in range(len(input)):
			part = input[i]
			if len(part) == 0:
				continue
			acum = self.__aggregatePartition(f, part, acum)
			input[i] = None

		partial_reduce.writeIterator().write(acum)
		self.__finalTreeReduce(f, partial_reduce)

	def union(self, other, preserveOrder):
		input = self._executor_data.getPartitions()
		input2 = self._executor_data.getVariable(other)
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		logger.info("Reduce: union " + str(len(input)) + " and " + str(len(input2)) + " partitions")
		if len(input) > 0:
			storage = input[0].type()
			if input[0].type() != input2[0].type():
				for i in range(len(input2)):
					new_part = self._executor_data.getPartitionTools().newPartition(storage)
					input2[i].copyTo(new_part)
					input2[i] = new_part
		else:
			storage = self._executor_data.getProperties().partitionType()

		if preserveOrder:
			logger.info("Reduce: union using order mode")
			executors = self._executor_data.getContext().executors()
			rank = self._executor_data.getContext().executorId()
			count = len(input)
			global_count = 0
			offset = 0
			count2 = len(input2)
			global_count2 = 0
			offset2 = 0
			exec_counts = self._executor_data.mpi().native().allgather(count)
			exec_counts2 = self._executor_data.mpi().native().allgather(count2)

			for i in range(executors):
				if i == rank:
					offset = global_count
					offset2 = global_count2
				global_count += exec_counts[i]
				global_count2 += exec_counts2[i]
			tmp = self._executor_data.getPartitionTools().newPartitionGroup()
			def create(n):
				for i in range(n):
					tmp.add(self._executor_data.getPartitionTools().newPartition(storage))
			def copy(parts):
				for part in parts:
					tmp.add(part)

			create(offset)
			copy(input)
			create(global_count - len(tmp))
			create(offset2)
			copy(input2)
			create(global_count + global_count2 - len(tmp))
			self.exchange(tmp, output)
		else:
			logger.info("Reduce: union using fast mode")
			for part in input:
				output.add(part)
			for part in input2:
				output.add(part)

		self._executor_data.setPartitions(output)

	def join(self, other, numPartitions):
		logger.info("Reduce: preparing first partitions")
		self.__keyHashing(numPartitions)
		self.__exchanging()
		input1 = self._executor_data.getAndDeletePartitions()

		logger.info("Reduce: preparing second partitions")
		self._executor_data.setPartitions(self._executor_data.getVariable(other))
		self._executor_data.removeVariable(other)
		self.__keyHashing(numPartitions)
		self.__exchanging()
		input2 = self._executor_data.getAndDeletePartitions()

		logger.info("Reduce: joining key elements")
		output = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)

		acum = dict()
		for p in range(len(input1)):
			for key, value in input1[p]:
				if key in acum:
					acum[key].append(value)
				else:
					acum[key] = [value]
			input1[p] = None
			writer = output[p].writeIterator()

			for key, value2 in input2[p]:
				if key in acum:
					for value1 in acum[key]:
						writer.write((key, (value1, value2)))
			input2[p] = None
			acum.clear()

		self._executor_data.setPartitions(output)

	def distinct(self, numPartitions):
		input = self._executor_data.getAndDeletePartitions()
		logger.info("Reduce: distinct" + str(len(input)) + " partitions")
		tmp = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
		logger.info("Reduce: creating" + str(numPartitions) + " new partitions with hashing")

		writers = [part.writeIterator() for part in tmp]

		for i in range(len(input)):
			for elem in input[i]:
				writers[hash(elem) % numPartitions].write(elem)
		del input

		output = self._executor_data.getPartitionTools().newPartitionGroup()

		self.__distinctFilter(tmp)
		self.exchange(tmp, output)
		self.__distinctFilter(output)
		self._executor_data.setPartitions(output)

	def groupByKey(self, numPartitions):
		self.__keyHashing(numPartitions)
		self.__exchanging()

		input = self._executor_data.getAndDeletePartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
		logger.info("Reduce: reducing key elements")

		acum = dict()
		for p in range(len(input)):
			for key, value in input[p]:
				if key in acum:
					acum[key].append(value)
				else:
					acum[key] = [value]
			input[p] = None
			writer = output[p].writeIterator()
			for item in acum.items():
				writer.write(item)
			acum.clear()

		self._executor_data.setPartitions(output)

	def reduceByKey(self, f, numPartitions, localReduce):
		context = self._executor_data.getContext()
		f.before(context)
		if localReduce:
			logger.info("Reduce: local reducing key elements")
			self.__localReduceByKey(f)
		self.__keyHashing(numPartitions)
		self.__exchanging()
		logger.info("Reduce: reducing key elements")

		self.__localReduceByKey(f)
		f.after(context)

	def aggregateByKey(self, f, numPartitions, hashing):
		context = self._executor_data.getContext()
		f.before(context)
		if hashing:
			self.__keyHashing(numPartitions)
			self.__exchanging()
		logger.info("Reduce: aggregating key elements")

		self.__localAggregateByKey(f)
		f.after(context)

	def foldByKey(self, f, numPartitions, localFold):
		context = self._executor_data.getContext()
		f.before(context)

		if localFold:
			logger.info("Reduce: local folding key elements")
			self.__localAggregateByKey(f)
			self.__keyHashing(numPartitions)
			self.__exchanging()
			logger.info("Reduce: folding key elements")
			self.__localReduceByKey(f)
		else:
			self.__keyHashing(numPartitions)
			self.__exchanging()
			logger.info("Reduce: folding key elements")
			self.__localAggregateByKey(f)

	def __reducePartition(self, f, part):
		context = self._executor_data.getContext()
		reader = part.readIterator()
		acum = reader.next()
		for item in reader:
			acum = f.call(acum, item, context)
		return acum

	def __finalReduce(self, f, partial):
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		# logger.info("Reduce: reducing all elements in the executor")
		# Python is single core, len(partial) is always 1
		logger.info("Reduce: gathering elements for an executor")
		self._executor_data.mpi().gather(partial, 0)
		if self._executor_data.mpi().isRoot(0) and len(partial) > 0:
			logger.info("Reduce: final reduce")
			result = self._executor_data.getPartitionTools().newMemoryPartition(1)
			result.writeIterator().write(self.__reducePartition(f, partial))
			output.add(result)
		self._executor_data.setPartitions(output)

	def __finalTreeReduce(self, f, partial):
		executors = self._executor_data.mpi().executors()
		rank = self._executor_data.mpi().rank()
		context = self._executor_data.getContext()
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		# logger.info("Reduce: reducing all elements in the executor")
		# Python is single core, len(partial) is always 1

		logger.info("Reduce: performing a final tree reduce")
		distance = 1
		order = 1
		while order < executors:
			order *= 2
			if rank % order == 0:
				other = rank + distance
				distance = order
				if other >= executors:
					continue
				self._executor_data.mpi().recv(partial, other, 0)
				result = f.call(partial[0], partial[1], context)
				partial.clear()
				partial.writeIterator().write(result)
			else:
				other = rank - distance
				self._executor_data.mpi().send(partial, other, 0)
				break

		if self._executor_data.mpi().isRoot(0) and len(partial) > 0:
			result = self._executor_data.getPartitionTools().newMemoryPartition(1)
			result.writeIterator().write(partial[0])
			output.add(result)
		self._executor_data.setPartitions(output)

	def __aggregatePartition(self, f, part, acum):
		context = self._executor_data.getContext()
		for item in part:
			acum = f.call(acum, item, context)
		return acum

	def __localReduceByKey(self, f):
		input = self._executor_data.getPartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		context = self._executor_data.getContext()
		acum = dict()
		for p in range(len(input)):
			for key, value in input[p]:
				if key in acum:
					acum[key] = f.call(acum[key], value, context)
				else:
					acum[key] = value

		output.add(self._executor_data.getPartitionTools().newMemoryPartition())
		writer = output[0].writeIterator()
		for item in acum.items():
			writer.write(item)

		self._executor_data.setPartitions(output)

	def __localAggregateByKey(self, f):
		input = self._executor_data.getAndDeletePartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		context = self._executor_data.getContext()
		base_acum = self._executor_data.getVariable("zero")

		acum = dict()
		for p in range(len(input)):
			for key, value in input[p]:
				if key in acum:
					acum[key] = f.call(acum[key], value, context)
				else:
					acum[key] = f.call(base_acum, value, context)
			input[p] = None

		output.add(self._executor_data.getPartitionTools().newMemoryPartition())
		writer = output[0].writeIterator()
		for item in acum.items():
			writer.write(item)

		self._executor_data.setPartitions(output)

	def __distinctFilter(self, parts):
		distinct = set()
		for i in range(len(parts)):
			new_part = self._executor_data.getPartitionTools().newPartition()
			writer = new_part.writeIterator()
			for elem in parts[i]:
				if elem not in distinct:
					writer.write(elem)
					distinct.add(elem)
			parts[i] = new_part
			distinct.clear()

	def __keyHashing(self, numPartitions):
		input = self._executor_data.getAndDeletePartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
		cache = input.cache()
		logger.info("Reduce: creating " + str(len(input)) + " new partitions with key hashing")

		writers = [part.writeIterator() for part in output]
		n = len(writers)
		for i in range(len(input)):
			part = input[i]
			for elem in part:
				writers[hash(elem[0]) % n].write(elem)
			if not cache:
				part.clear()
			input[i] = None

		self._executor_data.setPartitions(output)

	def __exchanging(self):
		input = self._executor_data.getPartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		numPartitions = len(input)
		logger.info("Reduce: exchanging " + str(numPartitions) + " partitions")

		self.exchange(input, output)

		self._executor_data.setPartitions(output)
