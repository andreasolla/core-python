class IPartition:

	def readIterator(self):
		pass

	def __iter__(self):
		return self.readIterator()

	def writeIterator(self):
		pass

	def read(self, transport):
		pass

	def write(self, transport, compression=0, native=None):
		pass

	def clone(self):
		pass

	def copyFrom(self, source):
		pass

	def copyTo(self, target):
		target.copyFrom(self)

	def moveFrom(self, source):
		pass

	def moveTo(self, target):
		target.moveFrom(self)

	def size(self):
		pass

	def __len__(self):
		return self.size()

	def empty(self):
		return self.size() == 0

	def __nonzero__(self):
		return self.size() != 0

	def bytes(self):
		pass

	def clear(self):
		pass

	def fit(self):
		pass

	def type(self):
		pass


class IPartitionGroup:

	def __init__(self):
		self.__partitions = list()
		self.__cache = False

	def __setitem__(self, index, value):
		self.__partitions[index] = value

	def __getitem__(self, index):
		return self.__partitions[index]

	def __delitem__(self, index):
		del self.__partitions[index]

	def __iter__(self):
		return self.__partitions.__iter__()

	def partitions(self):
		return len(self.__partitions)

	def __len__(self):
		return self.__partitions.__len__()

	def empty(self):
		return self.size() == 0

	def __nonzero__(self):
		return self.size() != 0

	def add(self, partition):
		self.__partitions.append(partition)

	def remove(self, index):
		del self.__partitions[index]

	def clear(self):
		self.__partitions.clear()

	def clone(self):
		copy = IPartitionGroup()
		for p in self.__partitions:
			copy.add(p.clone())
		return copy

	def shadowCopy(self):
		copy = IPartitionGroup()
		for p in self.__partitions:
			copy.add(p)
		return copy

	def cache(self, value=None):
		if value is not None:
			self.__cache = value
		return self.__cache


def copy(readIterator, writeIterator):
	n = 0
	while readIterator.hasNext():
		writeIterator.write(readIterator.next())
		n += 1
	return n
