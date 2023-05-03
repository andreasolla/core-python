import os
import sys
import unittest
from pathlib import Path

import coverage
from mpi4py import MPI


def main(path, parallel):
	cov = coverage.coverage(
		branch=True,
		include=str(Path(path).parent) + '/ignis/executor/*.py',
	)
	cov.start()
	import ignis.executor.core.ILog as Ilog
	Ilog.enable(False)
	tests = unittest.TestLoader().discover(path + '/executor/core', pattern='*Test.py')
	if parallel:
		tests.addTests(unittest.TestLoader().discover(path + '/executor/core', pattern='IMpiTest2.py'))
	else:
		print("WARNING: mpi test skipped", file=sys.stderr)
	result = unittest.TextTestRunner(verbosity=2, failfast=True).run(tests)
	cov.stop()
	cov.save()
	MPI.COMM_WORLD.Barrier()
	if result.wasSuccessful() and result.testsRun > 0 and MPI.COMM_WORLD.Get_rank() == 0:
		if parallel:
			others = ["../np" + str(i) + "/.coverage" for i in range(1, MPI.COMM_WORLD.Get_size())]
			cov.combine(data_paths=others, strict=True)
		covdir = os.path.join(os.getcwd(), "ignis-python-coverage")
		print('Coverage: (HTML version: file://%s/index.html)' % covdir, file=sys.stderr)
		cov.report(file=sys.stderr)
		cov.html_report(directory=covdir)


if __name__ == '__main__':
	rank = MPI.COMM_WORLD.Get_rank()
	parallel = MPI.COMM_WORLD.Get_size() > 1
	path = os.getcwd()
	Path("debug").mkdir(parents=True, exist_ok=True)
	os.chdir("debug")
	if parallel:
		wd = "np" + str(rank)
		Path(wd).mkdir(parents=True, exist_ok=True)
		os.chdir(wd)
		if rank > 0:
			log = open("log.txt", 'w')
			sys.stderr = log
			sys.stdout = log
	main(path, parallel)
	if rank > 0:
		sys.stderr.close()
