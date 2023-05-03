import traceback

from ignis.rpc.executor.exception.ttypes import IExecutorException


class IModule:

	def __init__(self, executor_data, logger):
		self._executor_data = executor_data
		self.__logger = logger

	def _pack_exception(self, ex):
		message = str(ex)
		stack = traceback.format_exc()
		cause = ex.__class__.__name__ + ': ' + message + "\nCaused by: \n" + stack
		self.__logger.error(cause)
		raise IExecutorException(message=message, cause_=cause)

	def _use_source(self, src, opt=False):
		try:
			self._executor_data.loadLibrary(src).before(self._executor_data.getContext())
		except Exception as ex:
			self._pack_exception(ex)
