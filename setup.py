from setuptools import setup, find_packages

setup(
	name='ignis-core',
	version='1.0',
	description='Ignis python core',
	packages=find_packages(),
	install_requires=['thrift', 'cloudpickle', 'mpi4py'],
	tests_require=['coverage']
)
