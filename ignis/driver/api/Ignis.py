import logging
import subprocess
import threading

from ignis.driver.api.IDriverException import IDriverException
from ignis.driver.core.ICallBack import ICallBack
from ignis.driver.core.IClientPool import IClientPool

logger = logging.getLogger(__name__)


class Ignis:
    __lock = threading.Lock()
    __backend = None
    __pool = None
    __callback = None

    @classmethod
    def start(cls):
        try:
            with cls.__lock:
                if cls.__pool:
                    return
                cls.__backend = subprocess.Popen(["ignis-backend"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)

                backend_port = int(cls.__backend.stdout.readline())
                backend_compression = int(cls.__backend.stdout.readline())
                callback_port = int(cls.__backend.stdout.readline())
                callback_compression = int(cls.__backend.stdout.readline())

                cls.__callback = ICallBack(callback_port, callback_compression)
                cls.__pool = IClientPool(backend_port, backend_compression)
        except Exception as ex:
            raise IDriverException(str(ex)) from ex

    @classmethod
    def stop(cls):
        try:
            with cls.__lock:
                if not cls.__pool:
                    return
                with cls.__pool.getClient() as client:
                    client.getBackendService().stop()
                cls.__backend.wait()
                cls.__pool.destroy()
                try:
                    cls.__callback.stop()
                except Exception as ex:
                    logger.error(str(ex))

                cls.__backend = None
                cls._pool = None
                cls.__callback = None
        except Exception as ex:
            raise IDriverException(ex) from ex

    @classmethod
    def _clientPool(cls):
        if not cls.__pool:
            raise IDriverException("Ignis.start() must be invoked before the other routines")
        return cls.__pool

    @classmethod
    def _driverContext(cls):
        if not cls.__callback:
            raise IDriverException("Ignis.start() must be invoked before the other routines")
        return cls.__callback.driverContext()
