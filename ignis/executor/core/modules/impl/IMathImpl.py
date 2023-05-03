import logging
import math
import random

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl, MPI

logger = logging.getLogger(__name__)


class IMathImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data, logger)

	def sample(self, withReplacement, num, seed):
		input = self._executor_data.getAndDeletePartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup(len(input))

		logger.info("Math: sample " + str(len(input)) + " partitions")
		random.seed(seed)
		for p in range(len(input)):
			writer = output[p].writeIterator()
			part = input[p]
			sz = len(part)
			if not self._executor_data.getPartitionTools().isMemory(part):
				aux = self._executor_data.getPartitionTools().newMemoryPartition(sz)
				part.copyTo(aux)
				part = aux
			if withReplacement:
				for i in range(num[p]):
					j = int(random.uniform(0, 1) * (sz - 1))
					writer.write(part[j])
			else:
				picked = 0
				for i in range(sz):
					if num[p] - picked == 0:
						break
					prob = (num[p] - picked) / (sz - i)
					rand = random.uniform(0, 1)
					if rand < prob:
						writer.write(part[i])
						picked += 1
			input[p] = None

		self._executor_data.setPartitions(output)

	def count(self):
		n = 0
		input = self._executor_data.getPartitions()
		logger.info("Math: count " + str(len(input)) + " partitions")
		for part in input:
			n += len(part)
		self._executor_data.deletePartitions()
		return n

	def sampleByKeyFilter(self):
		input = self._executor_data.getAndDeletePartitions()
		tmp = self._executor_data.getPartitionTools().newPartitionGroup(len(input))
		fractions = self._executor_data.getContext().vars().get("fractions")

		logger.info("Math: filtering key before sample " + str(len(input)) + " partitions")

		for i in range(len(input)):
			writer = tmp[i].writeIterator()
			for key, value in input[i]:
				if key in fractions:
					writer.write((key,value))
			input[i] = None

		output = self._executor_data.getPartitionTools().newPartitionGroup()
		for part in tmp:
			if len(part) > 0:
				output.add(part)

		numPartitions = min(len(output), len(fractions))
		numPartitions = self._executor_data.mpi().native().allreduce(numPartitions, MPI.MAX)
		self._executor_data.setPartitions(output)
		return numPartitions

	def sampleByKey(self, withReplacement, seed):
		input = self._executor_data.getAndDeletePartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		fractions = self._executor_data.getContext().vars().get("fractions")
		num = [0 for _ in range(len(fractions))]
		pmap = dict()
		for key,_ in fractions.items():
			pmap[key] = len(output)
			output.add(self._executor_data.getPartitionTools().newPartition())
		logger.info("Math: sampleByKey copying values to single partitions")

		for part in input:
			for key,values in part:
				pos = pmap[key]
				num[pos] = int(len(values) * fractions[key])
				writer = output[pos].writeIterator()
				for value in values:
					writer.write((key,value))

		self._executor_data.setPartitions(output)
		self.sample(withReplacement, num, seed)

	def countByKey(self):
		input = self._executor_data.getPartitions()
		logger.info("Math: counting local keys " + str(len(input)) + " partitions")

		acum = dict()
		for part in input:
			for key, _ in part:
				if key in acum:
					acum[key] += 1
				else:
					acum[key] = 1

		self.__countByReduce(acum)

	def countByValue(self):
		input = self._executor_data.getPartitions()
		logger.info("Math: counting local value " + str(len(input)) + " partitions")

		acum = dict()
		for part in input:
			for _, value in part:
				if value in acum:
					acum[value] += 1
				else:
					acum[value] = 1

		self.__countByReduce(acum)

	def __countByReduce(self, acum):
		logger.info("Math: reducing global counting")
		executors = self._executor_data.mpi().executors()
		group = self._executor_data.getPartitionTools().newPartitionGroup(executors)
		tmp = self._executor_data.getPartitionTools().newPartitionGroup(executors)
		writers = [part.writeIterator() for part in group]
		for key,count in acum.items():
			writers[hash(key) % executors].write((key,count))
		self.exchange(group, tmp)
		acum.clear()
		for key,count in tmp[0]:
			if key in acum:
				acum[key] += 1
			else:
				acum[key] = 1

		part = self._executor_data.getPartitionTools().newPartition()
		writer = part.writeIterator()
		for item in acum.items():
			writer.write(item)

		output = self._executor_data.getPartitionTools().newPartitionGroup()
		output.add(part)
		self._executor_data.setPartitions(output)
