from typing import List, Tuple, Union, Dict, Any, Tuple, Callable
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
import threading
from time import time, sleep
from rdq import Q, Ch
import logging
import re
import uuid
import queue
from abc import ABC, abstractmethod


class DataFrameKeyConflict(Exception):
    """ Raised when dataframe exists with same key in a ``ThreadServer`` instance. """


class ThreadClient(ABC):
    """ Thread Client represents a threaded client. """
    @abstractmethod
    def fetch_range(self, start: float, stop: float):
        pass
    
    @abstractmethod
    def fetch_from(self, start: float, periods: int):
        pass

    @abstractmethod
    def fetch_up_to(self, end: float, periods: int):
        pass


class TaskRequest:
    """ Represents a single task request made by a client to the server. """
    def __init__(self, df_key: str, task_key: str):
        self.df_key = df_key
        self.task_key = task_key


class TaskResponse:
    """ Represents a single task response from the server. """
    def __init__(self, df_key: str):
        self.df_key = df_key


class ThreadDF:
    """ Represents a single Lockable DataFrame object in a threaded context """
    def __init__(self, df: pd.DataFrame, lock: threading.RLock):
        pass


class ThreadServer(ABC):
    """ Represents a threaded server instance. """

    def __init__(self, threads: int = mp.cpu_count(), data_dir: str = "/tmp", persist: bool = True,
                 flush_delay: float = 0.01, tasks=None, dataframes=None):
        """
        Args:
            data_dir = the location where we are storing dataframes
        """
        self._threads = threads or mp.cpu_count()
        self._data_dir = data_dir
        self._persist = persist
        self._flush_delay = flush_delay
        self._started: float = None
        self._running = False
        # the shared dataframe objects

        # the task functions that worker will execute against any of the dataframe objects
        self._tasks: Dict[str, Callable] = tasks or {}
        # list of active worker threads
        self._workers: List[threading.Thread] = []
        # stop signal used to stop the server
        self._stop_signal: threading.Event = None
        self.__data__map__: Dict[str, Tuple[pd.DataFrame, threading.RLock] = {}

    @property
    def uptime(self) -> float:
        """ Get the uptime for this server. """
        if not self._started:
            # server was never started. Uptime is 0
            return 0.0
        return time() - self._started

    def start(self):
        """ Start the thread server. """
        self._start()
        self._started = time()
        self._running = True
    
    def register_task(self, task_callable: Callable[[pd.DataFrame, threading.RLock], None]):
        """ Add ``task_callable`` to this server.

        Task callable must accept 2 args, a ``DataFrame`` object and a ``threading.RLock`` object.
        """
        self._tasks[task_callable.__qualname__] = task_callable

    def add_dataframe(self, df: pd.DataFrame, key: str = None):
        """ Add a ``DataFrame`` '``df``' to be processed by this server with an optional ``key``
        
        Raises:
            DataFrameKeyConflict when a key already exists in this instance.
        Returns:
            A string representing the key to this dataframe in this instance.
        """
        _key = key or uuid.uuid4().__str__()
        if _key in self._dataframes.keys():
            # raise conflict error if already exists.
            raise DataFrameKeyConflict(f"Key {_key} already exists in {self}")
        self._dataframes[_key] = df
        return _key

    @abstractmethod
    def new_client(self) -> ThreadClient:
        """ Get a new client for this server. """
        
        
    @abstractmethod
    def _start(self):
        """ Start the thread server. """



class RemoteServer(ThreadServer):
    """ Remote impelementation of ``Dfs``. """

    def __init__(self, redis: StrictRedis, dfs_id: str, buffer_size: int = 1,
                 threads: int = None, persist: bool = True, flush_delay: float = 0.1,
                 data_dir: str = None):

        self._redis = redis
        self._buffer_size = buffer_size or 1
        self._dfs_id = dfs_id
        self._req_queue = Q(self._redis, self._dfs_id + ":request", buffer=self._buffer_size)
        self._command_queue = Q(self._redis, self._dfs_id + ":command", buffer=1)
        self._threads = threads or mp.cpu_count()
        self._flush_delay = flush_delay or 0.01
        self._data_dir = data_dir

    def new_client(self):
        pass

