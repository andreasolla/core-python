class IDriverException(RuntimeError):
	def __init__(self, message, cause=None):
		self.__cause = cause
		if cause:
			message += "\n" + "Caused by: " + cause
		RuntimeError.__init__(self, message)

	def hasCause(self):
		return bool(self.__cause)

	def getCause(self):
		return self.__cause
