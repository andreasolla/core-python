import math
import re


class IPropertyParser:

	def __init__(self, properties):
		self.__properties = properties
		self.__bool = re.compile("y|Y|yes|Yes|YES|true|True|TRUE|on|On|ON")

	# All properties
	def cores(self):
		return self.getNumber("ignis.executor.cores")

	def transportCores(self):
		return self.getMinFloat("ignis.transport.cores", 0)

	def partitionMinimal(self):
		return self.getSize("ignis.partition.minimal")

	def sortSamples(self):
		return self.getMinFloat("ignis.modules.sort.samples", 0)

	def sortResampling(self):
		return self.getBoolean("ignis.modules.sort.resampling")

	def loadType(self):
		key = "ignis.modules.load.type"
		return key in self.__properties and self.getBoolean(key)

	def ioOverwrite(self):
		return self.getBoolean("ignis.modules.io.overwrite")

	def ioCompression(self):
		return self.getRangeNumber("ignis.modules.io.compression", 0, 9)

	def msgCompression(self):
		return self.getRangeNumber("ignis.transport.compression", 0, 9)

	def partitionCompression(self):
		return self.getRangeNumber("ignis.partition.compression", 0, 9)

	def nativeSerialization(self):
		return self.getString("ignis.partition.serialization") == "native"

	def partitionType(self):
		return self.getString("ignis.partition.type")

	def exchangeType(self):
		return self.getString("ignis.modules.exchange.type")

	def jobName(self):
		return self.getString("ignis.job.name")

	def jobDirectory(self):
		return self.getString("ignis.job.directory")

	def executorDirectory(self):
		return self.getString("ignis.executor.directory")

	# Auxiliary functions
	def getString(self, key):
		if key in self.__properties:
			return self.__properties[key]
		raise KeyError(key + " is empty")

	def getNumber(self, key):
		return int(self.getString(key))

	def getFloat(self, key):
		return float(self.getString(key))

	def getRangeNumber(self, key, min, max):
		value = self.getNumber(key)
		if min is not None and value < min:
			raise ValueError(key + " error " + str(value) + " is less than " + str(min))
		if max is not None and value > max:
			raise ValueError(key + " error " + str(value) + " is greater than " + str(max))
		return value

	def getMaxNumber(self, key, max):
		return self.getRangeNumber(key, None, max)

	def getMinNumber(self, key, min):
		return self.getRangeNumber(key, min, None)

	def getRangeFloat(self, key, min, max):
		value = self.getFloat(key)
		if min is not None and value < min:
			raise ValueError(key + " error " + str(value) + " is less than " + str(min))
		if max is not None and value > max:
			raise ValueError(key + " error " + str(value) + " is greater than " + str(max))
		return value

	def getMaxFloat(self, key, max):
		return self.getRangeFloat(key, None, max)

	def getMinFloat(self, key, min):
		return self.getRangeFloat(key, min, None)

	def __parseError(self, key, value, pos):
		raise ValueError(key + " parsing error " + value[pos] + "(" + str(pos + 1) + ") in " + value)

	def getSize(self, key):
		value = self.getString(key).strip()
		UNITS = "KMGTPEZY"
		decimal = False
		length = len(value)
		exp = 0
		i = 0
		while i < length:
			if '9' >= value[i] >= '0':
				i = i + 1
			elif not decimal and (value[i] == '.' or value[i] == ','):
				i = i + 1
				decimal = True
			else:
				break
		num = float(value[0:i])
		if i < length:
			if value[i] == ' ':
				i = i + 1
		if i < length:
			exp = UNITS.find(value[i].upper()) + 1
			if exp > 0:
				i = i + 1
		if i < length and exp > 0 and value[i] == 'i':
			i = i + 1
			base = 1024
		else:
			base = 1000
		if i < length:
			if value[i] == 'B':
				pass
			elif value[i] == 'b':
				num = num / 8
			else:
				self.__parseError(key, value, i)
			i = i + 1
		if i != length:
			self.__parseError(key, value, i)
		return math.ceil(num * math.pow(base, exp))

	def getBoolean(self, key):
		return self.__bool.match(self.getString(key))
