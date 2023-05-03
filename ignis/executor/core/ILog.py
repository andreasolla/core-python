import logging


def init():
	logging.basicConfig(level=logging.INFO,
	                    format='%(asctime)s <%(levelname)-s> [%(filename)-s:%(lineno)d] %(message)s',
	                    datefmt='%B %e, %Y %I:%M:%S %p',
	                    )


def enable(endable):
	if endable:
		init()
	else:
		logging.basicConfig(level=50)
