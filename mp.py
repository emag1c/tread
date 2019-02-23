from . import ThreadClient, ThreadServer, task_key
from typing import List, Tuple, Union, Dict, Any, Tuple, Callable, TYPE_CHECKING
from redis import StrictRedis
from datetime import datetime, timedelta
from filelock import Timeout, FileLock
import pandas as pd
from os import path, makedirs, access, R_OK, W_OK, getcwd, sep
from collections import OrderedDict
from operator import itemgetter
from redlock import RedLock, RedLockFactory
import xxhash
from dateutil import rrule, relativedelta
from collections import deque, OrderedDict
import multiprocessing as mp
from multiprocessing.managers import BaseManager
import threading
from time import sleep
from rdq import Q, Ch
import logging
import re
import uuid
import queue
from abc import ABC, abstractmethod


class DataFramePoxyTaskNotFound(Exception):
    """ Raised when task not found in this ``DSyncManager``. """


class DSyncManager(BaseManager):
    """ default multiprocessing manager for ``DfSync``. """
    if TYPE_CHECKING:
        # enable IDE hinting
        DataFrameProxy = DataFrameProxy
        RLock = mp.RLock


class DataFrameProxy:
    """ Proxy object used for processing DataFrame objects. """
    def __init__(self, dataframe: pd.DataFrame = None,
                 tasks: Dict[str: Callable[[pd.DataFrame, mp.RLock], None]]=None,
                 persist=False, data_path_fmt: str = None, sync_internval: float = 0.1,
                 logger: logging.Logger = None):
        """ Initialize the ``DfProxy`` object with a dataframe. """

        # internal dataframe
        self.__dataframe__: pd.DataFrame = dataframe
        # internal lock
        self.__lock__ = mp.RLock()
        # registered tasks
        self.__tasks__: Dict[str: Callable[[pd.DataFrame, mp.RLock], None]] = {}
        # whether we're storing data on disk or not
        self._persist = persist
        # interval for saving data to disk
        self._sync_interval = sync_internval
        # format expression for data path
        self._data_path_fmt = data_path_fmt
        # if no data path expression was provided, but persist is True, set to cwd
        if self._persist and not self._data_path_fmt:
            self._data_path_fmt = path.join(getcwd(), "{key}.hdf")

        if tasks:
            for task in tasks:
                self.add_task(task)
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

    def add_task(self, task: Callable[[pd.DataFrame, mp.RLock], None]):
        """ Add a task to this ``DfProxy`` instance.
        Task should accept a ``DataFrame`` object and a ``multiprocessing.RLock`` object.
        """
        self._logger.debug("Adding task: %s to %s", task.__qualname__, self)
        self.__tasks__[task.__qualname__] = task

    def run_task(self, key: str, *args, **kwargs):
        """ Run a task with given ``key`` str against a ``DataFrame`` with given ``df_key`` """
        
        if key in self.__tasks__.keys():
            self.__dataframe__ = self.__tasks__[key](self.__dataframe__, self.__lock__, *args,
                                                     **kwargs)
        elif hasattr(self.__dataframe__, key):
            self.__dataframe__ = getattr(self.__dataframe__, key)(*args, **kwargs)

        raise DataFramePoxyTaskNotFound(f"Task with key '{key}' was not found")


    # TODO: add thread daemon that saves dataframe as hdf5 file
    
# Regsiter the manager's types
DSyncManager.register('DataFrameProxy', DataFrameProxy)
DSyncManager.register('RLock', mp.RLock)


class DsyncServer:
    """ Dsync Server """
    def __init__(self, dataframes: Dict[str, pd.DataFrame]={}, tasks=None, persist=False,
                 workers: int = mp.cpu_count(), data_path_fmt: str = None,
                 sync_internval: float = 1.0, logger: logging.Logger = None):

        # the multiprocessing manager ``DSyncManager`` for accessing ``DataFrameProxy``` objects
        self.__manager__: 'DsyncManagerMock' = DSyncManager()
        # the internal set of tasks that are inherited by DataFrameProxy objects
        self.__tasks__: Dict[str, Callable[[pd.DataFrame, mp.RLock, list, dict], pd.DataFrame]] =\
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
        self._stop_event: mp.Event = None
        if self._persist and not self._data_path_fmt:
            self._data_path_fmt = path.join(getcwd(), "{key}.hdf")

    def add_data(self, **dataframes: Dict[str, pd.DataFrame]):
        """ Add data to the server. """
        self.__dataframes__.update(dataframes)

    def start(self):
        """ start the server. """
        # start the manager process

        self.__manager__.start()
        self._stop_event = mp.Event()
        self.__task_queue__ = mp.Queue(self._workers)
        self.__error_queue__ = mp.SimpleQueue()

        # add thr proxy objects
        for k, df in self.__dataframes__.items():
            self.__df_proxies__[k] = self.__manager__.DataFrameProxy(
                df,
                tasks=self.__tasks__,
                persist=self._persist,
                data_path_fmt=self._data_path_fmt,
                sync_internval=self._sync_interval,
                logger=self._logger)

        for i in range(self._workers):
            p = mp.Process(target=self._worker, args=(self.__task_queue__, self.__error_queue__),
                           daemon=True)
            p.start()
            self.__workers__.append(p)

    def _worker(self, q: mp.queues.Queue, err: mp.queues.Queue):
        """ The worker process target for handling requests. """
        
        while not self._stop_event.is_set():
            func, 

    def stop(self):
        """ stop the server. """
        # shutdown the manager
        self.__manager__.shutdown()
