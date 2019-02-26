import logging
import multiprocessing as mp
import pandas as pd
import queue
import re
import threading
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import datetime, timedelta
from multiprocessing.managers import BaseManager
from operator import itemgetter
from os import getcwd, path, sep
from time import sleep, time
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Tuple, Union


class DataFramePoxyTaskNotFound(Exception):
    """ Raised when task not found in this ``DSyncManager``. """


class DsyncServerAlreadyRunning(Exception):
    """ Raised when `DsyncServer`` is already running. """


class DsyncServerNotRunning(Exception):
    """ Raised when `DsyncServer`` is not running. """


class DSyncManager(BaseManager):
    """ default multiprocessing manager for ``DfSync``. """
    if TYPE_CHECKING:
        # enable IDE hinting
        DataFrameProxy = DataFrameProxy
        RLock = mp.RLock


class DataFrameProxy:
    """ Proxy object used for processing DataFrame objects. """
    def __init__(self, dataframe: pd.DataFrame = None,
                 persist=False, data_path_fmt: str = None, sync_internval: float = 0.1,
                 logger: logging.Logger = None):
        """ Initialize the ``DfProxy`` object with a dataframe. """

        # internal dataframe
        self.__dataframe__: pd.DataFrame = dataframe
        # internal lock
        self.__lock__ = mp.RLock()
        # whether we're storing data on disk or not
        self._persist = persist
        # interval for saving data to disk
        self._sync_interval = sync_internval
        # format expression for data path
        self._data_path_fmt = data_path_fmt
        # if no data path expression was provided, but persist is True, set to cwd
        if self._persist and not self._data_path_fmt:
            self._data_path_fmt = path.join(getcwd(), "{key}.hdf")
        # logger
        self._logger = logger or logging.getLogger(__name__)

    @property
    def df(self):
        """ Get the internal DataFrame object. """
        return self.__dataframe__

    @property
    def lock(self):
        """ Get the internal lock. """
        return self.__lock__

    def set_df(self, df: pd.DataFrame):
        """ Set a ``DataFrame`` with an optionally provided Key. """
        self.__dataframe__ = df

    def call(self, method_name: str, lock: bool, *args, **kwargs):
        """ Call a native dataframe method. """

        if lock:
            # if lock flag is was passed, then lock before calling method
            self.__lock__.acquire()
            res = getattr(self.__dataframe__, method_name)(*args, **kwargs)
            self.__lock__.release()
            return res

        return getattr(self.__dataframe__, method_name)(*args, **kwargs)

    def __getattr__(self, attr: str):
        """ try to call the attribute attached directly to the internal dataframe. """
        if hasattr(self.__dataframe__, attr):
            return getattr(self.__dataframe__, attr)

    # TODO: add thread daemon that saves dataframe as hdf5 file
    
# Regsiter the manager's types
DSyncManager.register('DataFrameProxy', DataFrameProxy)
DSyncManager.register('RLock', mp.RLock)


class DsyncStorageManager(ABC):
    """ Persistant data class responsible for syncing data to disk or memory. """
    # TODO: finish this class and extend for use with Redis or HDFS
    
    def __init__(self, sync_interval: float = 1.0):
        self._sync_interval = 1.0

    @abstractmethod
    def store(self, *args, **kwargs):
        pass

    @abstractmethod
    def load(self, *args, **kwargs):
        pass


class DsyncServer:
    """ Dsync Server """
    def __init__(self, dataframes: Dict[str, pd.DataFrame]={}, tasks=None, persist=False,
                 workers: int = mp.cpu_count(), data_path_fmt: str = None,
                 sync_internval: float = 1.0, logger: logging.Logger = None):

        # the multiprocessing manager ``DSyncManager`` for accessing ``DataFrameProxy``` objects
        self.__manager__: 'DsyncManagerMock' = DSyncManager()
        # the internal set of tasks that are inherited by DataFrameProxy objects
        self.__tasks__: Dict[str, Callable[[DataFrameProxy, list, dict], pd.DataFrame]] =\
            tasks or {}
        # the internal set of dataframe proxies
        self.__df_proxies__: Dict[str, DataFrameProxy] = {}
        # pre-loaded data
        self.__dataframes__: Dict[str, pd.DataFrame] = dataframes or {}
        # master job queue 
        self.__task_queue__: mp.queues.Queue = None
        self.__error_queue__: mp.queues.SimpleQueue = None
        # active processes
        self.__workers__: Dict[int, mp.Process]
        # whether or not to save data to disk
        self._persist = persist
        # the data path fmt to use for persistent data files
        self._data_path_fmt = data_path_fmt
        # how often data is flushed to disk in the background, in seconds, Default to 1 second
        self._sync_interval = sync_internval or 1.0
        # logger to use for this server
        self._logger = logger or logging.getLogger(__name__)
        # the total number of workers
        self._workers = workers or mp.cpu_count()
        # if no data path expression was provided, but persist is True, set to cwd
        self._stop_event = mp.Event()
        self._running_event = mp.Event()

        if self._persist and not self._data_path_fmt:
            self._data_path_fmt = path.join(getcwd(), "{key}.hdf")

    def add_data(self, **dataframes: Dict[str, pd.DataFrame]):
        """ Add ``dataframes`` to the server. """

        if self._running_event.isSet():
            raise DsyncServerAlreadyRunning("Cannot add data to a server that is already running.")

        self.__dataframes__.update(dataframes)

    def get_data(self, data_key: str) -> DataFrameProxy:
        """ get a ``DataFrameProxy`` object with = ``data_key``. """

        if not self._running_event.isSet():
            raise DsyncServerNotRunning("Server must be running to access any dataframe proxies.")

        return self.__df_proxies__[data_key]

    def add_tasks(self, *args):
        """ Add one or more ``callables`` to the task map. """

        for f in args:
            self.__tasks__[f.__qualname__] = f

    def start(self):
        """ start the server. """
        # start the manager process

        self.__manager__.start()
        self.__task_queue__ = mp.Queue(self._workers)
        self.__error_queue__ = mp.Queue(1)

        self._running_event.set()

        # add thr proxy objects
        for k, df in self.__dataframes__.items():
            self.__df_proxies__[k] = self.__manager__.DataFrameProxy(
                df,
                persist=self._persist,
                data_path_fmt=self._data_path_fmt,
                sync_internval=self._sync_interval,
                logger=self._logger)

        for _ in range(self._workers):
            # start the workers
            p = mp.Process(target=self._worker, args=(self.__task_queue__, self.__error_queue__),
                           daemon=True)
            p.start()
            self.__workers__[p.pid] = p

    def _worker(self, q: mp.queues.Queue, err: mp.queues.Queue):
        """ The worker process target for handling requests. """
        
        while not self._stop_event.is_set():
            try:
                df_key, task_key, result_queue, args, kwargs = q.get()

                if df_key not in self.__df_proxies__.keys():
                    raise ValueError(f"DataFrameProxy with key of '{df_key}' does not exist.")

                if task_key not in self.__tasks__.keys():
                    raise ValueError(f"Task function with qualname of '{task_key}' "
                                     "does not exist in task map.")

                result_queue.put(self.__tasks__[task_key](self.__df_proxies__[df_key], *args,
                                                          **kwargs))

            except (KeyboardInterrupt, SystemExit):
                # keybaord interput or system exit, just stop and return
                self._stop_event.set()
                break

            except Exception as e:
                # other exception, put the exception in the error queue and set system stop flag
                # self.__error_queue__.put(e)
                self._stop_event.set()
                self.__error_queue__.put(e)
                self._logger.error("Worker got error: %s", e)

    def run(self, dataframe_key: str, handler_key: str, *args, **kwargs) -> mp.SimpleQueue:
        """ Run a task on the server.
        
        Returns:
            The DataFrameProxy object that matches the dataframe_key
        """

        if not self._running_event.isSet():
            raise DsyncServerNotRunning("Server must be running to execute tasks.")

        if not isinstance(handler, str):
            handler = handler.__qualname__

        result_q = mp.SimpleQueue()
        self.__task_queue__.put((dataframe_key, handler_key, result_q, args, kwargs))
        return result_q

    def run_all(self, handler_key: str, *args, **kwargs) -> Dict[str, mp.SimpleQueue]:
        """ Run a task on the server.for all dataframes
        
        Returns:
            A dict of DataFrameProxy objects
        """
        
        if not self._running_event.isSet():
            raise DsyncServerNotRunning("Server must be running to execute tasks.")

        if not isinstance(handler, str):
            handler = handler.__qualname__
        
        queues = {}

        for k in self.__df_proxies__.keys():
            result_q = mp.SimpleQueue()
            self.__task_queue__.put((k, handler_key, result_q, args, kwargs))
            queues[k] = result_q

        return queues

    # def call(self, dataframe_key, )

    def stop(self, timeout: float = 1.0):
        """ stop the server. """
        # shutdown the manager

        timeout = timeout or 1.0

        self._stop_event.set()
        self._running_event.clear()

        _pool_pids = list(self.__workers__.keys())

        _t = time()
        while True:
            for pid in _pool_pids:
                if pid in self.__workers__.keys():
                    if self.__workers__[pid].is_alive():
                        continue
                    self.__workers__[pid].join()
                    self.__workers__[pid].terminate()
                    del self.__workers__[pid]

            if _t - time() > timeout:
                # timed out
                self._logger.warn("Stop server timed out. Killing all now")
                for pid in _pool_pids:
                    if pid in self.__workers__.keys():
                        self.__workers__[pid].join()
                        self.__workers__[pid].terminate()
                        self._logger.debug("Forced killed %s", pid)
                break
            
            if not self.__workers__:
                self._logger.debug("All processes exited cleanly.")
                break

        self.__manager__.shutdown()

        if not self.__error_queue__.empty():
            # if an error was raised before stop was called, raise it here
            raise self.__error_queue__.get()

    def is_running(self) -> bool:
        """ Check if server is still running. """
        return self._running_event.isSet()
    
    def get_error(self) -> Exception:
        """ Get an exception raised by a worker or None. """
        if not self.__error_queue__.empty():
            # if an error was raised before stop was called, raise it here
            return self.__error_queue__.get()
