from ignis.executor.api.function.IFunction import IFunction


class IVoidFunction(IFunction):

	def before(self, context):
		pass

	def call(self, v, context):
		pass

	def after(self, context):
		pass
