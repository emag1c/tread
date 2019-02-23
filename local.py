from . import ThreadClient, ThreadServer, task_key
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
from time import sleep
from rdq import Q, Ch
import logging
import re
import uuid
import queue
from abc import ABC, abstractmethod



class LocalServer(ThreadServer):
    """ Server extends base class to include the daemon processes that handle requests. """
    def __init__(self, threads: int = None, buffer: int = 1, persist: bool = True,
                 flush_delay: float = 0.1, data_dir: str = None, handlers: List[Callable] = None):

        # set super properties
        super().__init__(redis, freq)
        # default number of processes is the cpu count
        self._threads = threads or mp.cpu_count()
        self._persist = persist is True
        self._flush_delay = flush_delay or 0.1
        self._buffer = buffer

        self._req_queue: queue.Queue = None
        self._error_queue: queue.Queue = None
        self._workers: List[threading.Thread] = []
        self._stop_signal: threading.Event = None
        self._handlers = {}
        self._running = False

        if handlers:
            for handler in handlers:
                self.add_handler(handler)

    def add_handler(self, func: Union[Callable, Tuple[object, Callable]]):
        """ Add a handler for this local server. """
        self._handlers[task_key(func)] = func

    def new_client(self):
        """ Get a new client for this server. """
        return LocalClient(self.redis, self.freq, self._req_queue)

    def start(self):
        """ Start the server. """
        self.__start()

    def stop(self):
        """ Stop the server, terminate all threads. """
        self._stop_signal.set()
        for worker in self._workers:
            worker.join()
        self._running = False

        if not self._error_queue.empty():
            # if an error was raised by a worker, raise it here
            err = self._error_queue.get()
            raise err

    def __start(self):
        """ Start a daemon thread that keeps the server alive
         and start the master trhead pool that will handle queries
        """
        
        # create the queue object
        if self._running:
            print("Already running")
            return

        self._running = True
        self._req_queue = queue.Queue(self._buffer)
        self._error_queue = queue.Queue()
        
        for i in range(self._threads):
            # create the threads, pass the request and error threads to them
            t = threading.Thread(target=self._req_handler, args=(self._req_queue,
                                                                 self._error_queue))
            t.setDaemon(True)
            t.start()
            self._workers.append(t)

    def _req_handler(self, q: queue.Queue, err: queue.Queue):
        """ Handle requests in the queue and return resutls. """
        
        def raise_error(err):
            # raise an error inside this thread
            err.put(err)
            # stop the daemon
            self._stop_signal.set()

        while not self._stop_signal.is_set():
            # keep running until stop_thread event is set
            func, response_queue, *args = q.get()

            if func not in self._handlers.keys():
                raise_error(Exception(f"Handler function {func} was not regisetered."))
                return

            response_queue.put(self._handlers[func](*args))

    def run(handler, *args):
        response_queue = queue.Queue(1)
        self._queue.put()


class LocalClient(DfsClient):

    def __init__(self, redis: StrictRedis, freq: int, req_queue: queue.Queue):
        super().__init__(redis, freq)
        # set the queue created by the server
        self._queue = req_queue

    def fetch_range(self, start: float, stop: float):
        res_q = queue.Queue(1)
        item = self._queue.put((REQ_FETCH_RANGE, res_q, start, stop))
        return res_q.get()
    
    def fetch_from(self, start: float, periods: int):
        res_q = queue.Queue(1)
        item = self._queue.put((REQ_FETCH_FROM, res_q, start, periods))
        return item.get()

    def fetch_up_to(self, end: float, periods: int):
        res_q = queue.Queue(1)
        item = self._queue.put((REQ_FETCH_TO, res_q, end, periods))
        return item.get()
