import pickle


class INativeWriter:

	@classmethod
	def write(cls, protocol, obj):
		pickle.dump(obj=obj, file=protocol.trans, protocol=pickle.HIGHEST_PROTOCOL)
