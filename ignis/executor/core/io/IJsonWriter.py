import collections.abc
import json


class __IJsonWriterAbs(type):
	__types = dict()

	@classmethod
	def __setitem__(cls, key, value):
		cls.__types[key] = value

	@classmethod
	def __getitem__(cls, key):
		return cls.__types[key]

	@classmethod
	def __delitem__(cls, key):
		del cls.__types[key]

	@classmethod
	def _getWriterType(cls, obj):
		return cls.__types.get(type(obj), None)


class IJsonWriter(json.JSONEncoder, metaclass=__IJsonWriterAbs):

	def default(self, o):
		writer = IJsonWriter._getWriterType(o)
		if writer:
			return writer(o)
		elif isinstance(o, collections.abc.Iterable):
			class StreamArray(list):
				def __iter__(self):
					return iter(o)

				def __len__(self):
					return 1

			return StreamArray()
		else:
			return o.__dict__
