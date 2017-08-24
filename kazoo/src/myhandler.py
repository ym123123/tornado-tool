import threading 
import tornado.gen
from tornado.concurrent import Future
from kazoo.handlers.threading import SequentialThreadingHandler, KazooTimeoutError
from kazoo.client import KazooClient
from kazoo import python2atexit 
from kazoo.security import Permissions

_NONE = object()

class AsyncResult(object):
    """A one-time event that stores a value or an exception"""
    def __init__(self, handler, condition_factory, timeout_factory):
        self._handler = handler
        self._exception = _NONE
        self._condition = condition_factory()
        self._callbacks = []
        self._timeout_factory = timeout_factory
        self.value = None
        self.future = None
        
    def ready(self):
        """Return true if and only if it holds a value or an
        exception"""
        return self._exception is not _NONE

    def successful(self):
        """Return true if and only if it is ready and holds a value"""
        return self._exception is None
    
    def get_future(self):
        if self.future == None:
            self.future = Future()
            
            try:
                result = self.get_nowait()
            except KazooTimeoutError:
                result = None
            
            if result != None:
                #如果已经有结果了， 则重新设置
                tornado.ioloop.IOLoop.instance().add_callback(\
                lambda future:future.set_result(result), self.future)
                
        return self.future
        
    @property
    def exception(self):
        if self._exception is not _NONE:
            return self._exception

    def set(self, value=None):
        """Store the value. Wake up the waiters."""
        with self._condition:
            self.value = value
            self._exception = None
            for callback in self._callbacks:
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
            self._condition.notify_all()
            if self.future:
                self.future.set_result(value)

    def set_exception(self, exception):
        """Store the exception. Wake up the waiters."""
        with self._condition:
            self._exception = exception
            for callback in self._callbacks:
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
            self._condition.notify_all()
            
            if self.future:
                self.future.set_exception(exception)

    def get(self, block=True, timeout=None):
        """Return the stored value or raise the exception.

        If there is no value raises TimeoutError.

        """
        with self._condition:
            if self._exception is not _NONE:
                if self._exception is None:
                    return self.value
                raise self._exception
            elif block:
                self._condition.wait(timeout)
                if self._exception is not _NONE:
                    if self._exception is None:
                        return self.value
                    raise self._exception

            # if we get to this point we timeout
            raise self._timeout_factory()

    def get_nowait(self):
        """Return the value or raise the exception without blocking.

        If nothing is available, raises TimeoutError

        """
        return self.get(block=False)

    def wait(self, timeout=None):
        """Block until the instance is ready."""
        with self._condition:
            self._condition.wait(timeout)
        return self._exception is not _NONE

    def rawlink(self, callback):
        """Register a callback to call when a value or an exception is
        set"""
        with self._condition:
            # Are we already set? Dispatch it now
            if self.ready():
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
                return

            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def unlink(self, callback):
        """Remove the callback set by :meth:`rawlink`"""
        with self._condition:
            if self.ready():
                # Already triggered, ignore
                return

            if callback in self._callbacks:
                self._callbacks.remove(callback)

class MyHandler(SequentialThreadingHandler):
    __handler = None
    def __init__(self):
        SequentialThreadingHandler.__init__(self)
    
    def async_result(self):
        return AsyncResult(self, threading.Condition,
                                          KazooTimeoutError)
    @staticmethod
    def get_zookeeper(hosts='127.0.0.1:2181'):
        if MyHandler.__handler == None:
            handler = MyHandler()
            MyHandler.__handler = KazooClient(hosts = hosts, handler = handler)
            MyHandler.__handler.start(1)
            python2atexit.register(lambda : MyHandler.__handler.stop())
        return MyHandler.__handler

@tornado.gen.coroutine
def get_child_state_watcher(zk, path):
    future = Future()
    def watcher(r):
        future.set_result(r)
    yield zk.get_children_async(path, watcher).get_future()
    result = yield future
    return result

@tornado.gen.coroutine 
def get_child_data_watcher(zk, path):
    future = Future()
    
    def watcher(r):
        future.set_result(r)
    yield zk.exists_async(path, watcher).get_future()
    result = yield future
    return result
