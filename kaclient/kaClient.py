from kazoo.client import KazooClient
import tornado.gen
import concurrent.futures

class TKAClient(KazooClient):
    def __init__(self, host='127.0.0.1:2181', timeout=2):
        super(TKAClient, self).__init__(host, timeout)
    
    @tornado.gen.coroutine 
    def stop(self):
        KazooClient.stop(self)
    
    @tornado.gen.coroutine 
    def start(self, timeout=5):
        self.start_async()
        num = timeout / 0.1 
        
        while num:
            if self.connected:
                return 
            yield tornado.gen.sleep(0.1)
            num = num - 1
        KazooClient.start(self, 0)
        
    @tornado.gen.coroutine 
    def restart(self):
        yield self.stop()
        yield self.start()
    
    @tornado.gen.coroutine 
    def create(self, path, value=b"", acl=None, ephemeral=False, 
        sequence=False, makepath=False):
        event = self.create_async(path, value, acl, ephemeral, sequence, makepath)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def add_auth(self, scheme, credential):
        event = self.add_auth_async(scheme, credential)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def sync(self, path):
        event = self.sync_async(path)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def ensure_path(self, path, acl=None):
        event = self.ensure_path(path, acl)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def exists(self, path):
        event = self.exists_async(path)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def exist_change(self, path):
        f = concurrent.futures.Future()
        event = self.exists_async(path, lambda data = None: f.set_result(data))
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return((event.get_nowait(), f))
    
    @tornado.gen.coroutine 
    def get(self, path):
        event = self.get_async(path)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def get_change(self, path):
        f = concurrent.futures.Future()
        event = self.get_async(path, lambda data = None: f.set_result(data))
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return((event.get_nowait(), f))
    
    @tornado.gen.coroutine 
    def get_children(self, path):
        event = self.get_children_async(path)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def get_children_change(self, path):
        f = concurrent.futures.Future()
        event = self.get_children_async(path, lambda data = None, future = f: future.set_result(data))
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return((event.get_nowait(), f))
    
    @tornado.gen.coroutine 
    def get_acls(self, path):
        event = self.get_acls_async(path)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def set_acls(self, path, acls, version=-1):
        event = self.set_acls_async(path, acls, version)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine
    def set(self, path, value, version):
        event = self.set_async(path, value, version)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    
    @tornado.gen.coroutine 
    def delete(self, path, version=-1):
        event = self.delete_async(path, version)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
        self.ChildrenWatch
    
    @tornado.gen.coroutine 
    def reconfig(self, joining, leaving, new_members, from_config=-1):
        event = self.reconfig_async(joining, leaving, new_members, from_config)
        yield tornado.gen.Task(event.rawlink)
        raise tornado.gen.Return(event.get_nowait())
    