import hashlib
import importlib.util
import logging
import os

import cloudpickle

logger = logging.getLogger(__name__)


class ILibraryLoader:

	def __init__(self, properties):
		self.__properties = properties
		self.__functions = dict()

	def loadFuntion(self, name):
		if name.startswith("lambda") or name.startswith("def"):
			return self.__loadLambda(name)
		elif ":" not in name:
			if name in self.__functions:
				return self.__functions[name]()
			raise KeyError("Function " + name + " not found")
		try:
			values = name.split(":")
			path = values[0]
			className = values[1]
			spec = importlib.util.spec_from_file_location(name=className, location=path)
			module = importlib.util.module_from_spec(spec)
			spec.loader.exec_module(module)
			classObject = getattr(module, className)
			self.__functions[className] = classObject
			return classObject()
		except Exception as ex:
			raise KeyError("Function " + name + " not found") from ex

	def loadLibrary(self, path):
		if "\n" in path:
			path = self.__loadSource(path)
		if not os.path.exists(path):
			raise RuntimeError(path + " not found")
		spec = importlib.util.spec_from_file_location(name="", location=path)
		if spec is None:
			raise RuntimeError(path + " is malformed")
		module = importlib.util.module_from_spec(spec)
		spec.loader.exec_module(module)
		try:
			functions = getattr(module, '__ignis_library__')
		except TypeError as ex:
			raise KeyError("'__ignis_library__' is required to register a python file as ignis library")

		if not isinstance(functions, list):
			raise TypeError("'__ignis_library__' must be a list")

		for name in functions:
			try:
				self.__functions[name] = getattr(module, name)
			except TypeError as ex:
				logger.warning(name + " not found in " + path + " ignoring")

	def __loadLambda(self, src):
		if src.startswith("lambda"):
			f = eval(src)
			return IFunctionLambda(f)
		result = {}
		exec(src, None, result)
		if len(result) > 1:
			raise RuntimeError("Only one function can be defined inside lambda code, found: " + " ".join(result.keys()))
		f = next(iter(result.values()))
		return IFunctionDef(f)

	def __loadSource(self, src):
		folder = os.path.join(self.__properties.executorDirectory(), "pysrc")
		os.makedirs(folder, exist_ok=True)
		file = os.path.join(folder, hashlib.sha1(src.encode()).hexdigest())
		file2 = file
		file += ".py"
		i = 0
		while os.path.exists(file):
			with open(file, "r") as other:
				if src == other.read():
					break
			i += 1
			file = file2 + str(i) + ".py"
		with open(file, "w") as output:
			output.write(src)
		return file

	@classmethod
	def unpickle(cls, bytes):
		return cloudpickle.loads(bytes)

	@classmethod
	def pickle(cls, src):
		return cloudpickle.dumps(src)


class IFunctionDef:

	def __init__(self, f):
		self.call = f

	def before(self, context):
		pass

	def after(self, context):
		pass


class IFunctionLambda:

	def __init__(self, f):
		self.f = f

	def before(self, context):
		pass

	def call(self, *args):
		return self.f(*args[:-1])

	def after(self, context):
		pass
