import threading 
import tornado.gen
from tornado.concurrent import Future
from kazoo.handlers.threading import SequentialThreadingHandler, KazooTimeoutError
from kazoo.client import KazooClient

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
        self.future = Future() 
        #异常极端的情况
        with self._condition:
            if self._exception is not _NONE:
                if self._exception is None:
                    tornado.ioloop.IOLoop.instance().add_callback(\
                        lambda future:future.set_result(self.value), self.future)
                else:
                    tornado.ioloop.IOLoop.instance().add_callback(\
                        lambda future:future.set_result(self.exception()), self.future)
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
                tornado.ioloop.IOLoop.instance().add_callback(\
                lambda future:future.set_result(value), self.future)

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
                tornado.ioloop.IOLoop.instance().add_callback(\
                lambda future:future.set_result(exception), self.future)

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
    
    def __init__(self, hosts='127.0.0.1:2181'):
        SequentialThreadingHandler.__init__(self)
        self.handler = KazooClient(hosts = hosts, handler = self)
    
    def async_result(self):
        return AsyncResult(self, threading.Condition,
                                          KazooTimeoutError)
        
    #这个函数需要全局初始化，如果你在tornado中调用， 可能造成严重问题
    @tornado.gen.coroutine
    def get_zookeeper(self, timeout = 1):
        self.handler.start_async()
        count = timeout / 0.05
        
        while count:
            count = count - 1
            
            if self.handler.connected:
                break 
            yield tornado.gen.sleep(0.05)
        
        if not self.handler.connected:
            self.handler.stop()
            raise self.handler.handler.timeout_exception("Connection time-out")
        
        return self.handler

@tornado.gen.coroutine
def __get_child_state_watcher(zk, path):
    future = Future()
    def watcher(r):
        tornado.ioloop.IOLoop.instance().add_callback(\
            lambda future:future.set_result(r), future)
    yield zk.get_children_async(path, watcher).get_future()
    result = yield future
    return result

@tornado.gen.coroutine
def get_child_state_watcher(zk, path):
    while True:
        try:
            result = yield __get_child_state_watcher(zk, path)
            return result
        except:
            yield tornado.gen.sleep(1)

@tornado.gen.coroutine 
def __get_child_data_watcher(zk, path):
    future = Future()
    
    def watcher(r):
        tornado.ioloop.IOLoop.instance().add_callback(\
            lambda future:future.set_result(r), future)
    yield zk.exists_async(path, watcher).get_future()
    result = yield future
    return result
@tornado.gen.coroutine
def get_child_data_watcher(zk, path):
    while True:
        try:
            result = yield __get_child_data_watcher(zk, path)
            return result
        except:
            yield tornado.gen.sleep(1)