import tornado.gen 
from kazoo.client import KazooClient
import six
from kazoo.exceptions import ConnectionLoss, KazooException
from tornado.iostream import IOStream
import re 
from tornado.concurrent import Future
ENVI_VERSION = re.compile('([\d\.]*).*', re.DOTALL)
ENVI_VERSION_KEY = 'zookeeper.version'
import socket

class kaClient(object):
	def __init__(self, ip = '127.0.0.1', port = 2181, session_timeout = 10):
		self.ip = ip 
		self.port = port 
		self.timeout = session_timeout
		self.client = KazooClient(hosts=ip + ':' + str(port))
		
	@tornado.gen.coroutine 
	def __start_async(self, timeout):
		yield tornado.gen.sleep(timeout)
		raise tornado.gen.Return(self.client.connected)
		
	@tornado.gen.coroutine 
	def start(self, timeout = 10):
		if timeout <= 0:
			self.client.start(timeout)
			return 
		self.client.start_async()
		
		count = timeout / 0.1
		while count:
			flag = yield self.__start_async(0.1)
			if flag:
				raise tornado.gen.Return(None)
			
			count = count - 1
	
		self.client.stop()
		raise self.client.handler.timeout_exception("Connection time-out")
		
	@tornado.gen.coroutine 
	def stop(self):
		self.client.stop()
	
	
	@tornado.gen.coroutine 
	def retry(self):
		yield self.stop()
		yield self.start()
	
	@tornado.gen.coroutine 
	def create(self, path, value=b"", acl=None, ephemeral=False,
                     sequence=False, makepath=False):
		event = self.client.create_async(path, value, acl, ephemeral, sequence, makepath)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	@tornado.gen.coroutine 
	def reconfig(self, joining, leaving, new_members, from_config):
		event = self.client.reconfig_async(joining, leaving, new_members, from_config)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	
	@tornado.gen.coroutine 
	def client_state(self):
		raise tornado.gen.Return(self.client.client_state)
	@tornado.gen.coroutine 
	def client_id(self):
		raise tornado.gen.Return(self.client.client_id)
	@tornado.gen.coroutine 
	def connected(self):
		raise tornado.gen.Return(self.client.connected)
	
	@tornado.gen.coroutine 
	def set_hosts(self, hosts, randomize_hosts=None):
		self.client.set_hosts(hosts, randomize_hosts)
	
	
	@tornado.gen.coroutine 
	def add_listener(self, listener):
		self.client.add_listener(listener)
	@tornado.gen.coroutine 
	def remove_listener(self, listener):
		self.client.remove_listener(listener)
		
	@tornado.gen.coroutine 
	def close(self):
		self.client.close()
		
	@tornado.gen.coroutine 
	def command(self, cmd=b'ruok'):
		flag = yield self.connected()
		
		if flag == False:
			raise ConnectionLoss("No connection to server")
		
		sfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
		stream = IOStream(sfd)
		try:
			yield stream.connect((self.ip, self.port))
			yield tornado.gen.with_timeout(self.timeout, stream.write(cmd))
			result = yield tornado.gen.with_timeout(self.timeout, stream.read_bytes(8192))
			raise tornado.gen.Return(result.decode('utf-8', 'replace'))
		finally:
			stream.close()
		
	@tornado.gen.coroutine 
	def server_version(self, retries=3):
		@tornado.gen.coroutine 
		def _try_fetch():
			data = yield self.command(b'envi')
			data_parsed = {}
			for line in data.splitlines():
				try:
					k, v = line.split("=", 1)
					k = k.strip()
					v = v.strip()
				except ValueError:
					pass
				else:
					if k:
						data_parsed[k] = v
			version = data_parsed.get(ENVI_VERSION_KEY, '')
			version_digits = ENVI_VERSION.match(version).group(1)
			try:
				raise tornado.gen.Return(tuple([int(d) for d in version_digits.split('.')]))
			except ValueError:
				raise tornado.gen.Return(None)
			
		def _is_valid(version):
			# All zookeeper versions should have at least major.minor
			# version numbers; if we get one that doesn't it is likely not
			# correct and was truncated...
			if version and len(version) > 1:
				return True
			return False

		# Try 1 + retries amount of times to get a version that we know
		# will likely be acceptable...
		version = yield _try_fetch()
		if _is_valid(version):
			raise tornado.gen.Return(version)
		for _i in six.moves.range(0, retries):
			version = yield _try_fetch()
			if _is_valid(version):
				raise tornado.gen.Return(version)
		raise KazooException("Unable to fetch useable server"
							 " version after trying %s times"
							 % (1 + max(0, retries)))


	@tornado.gen.coroutine 
	def add_auth(self, scheme, credential):
		event = self.client.add_auth_async(scheme, credential)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
		
	@tornado.gen.coroutine 
	def unchroot(self, path):
		raise tornado.gen.Return(self.client.unchroot(path))
	
	@tornado.gen.coroutine 
	def sync(self, path):
		event = self.client.sync_async(path)
		yield  tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	@tornado.gen.coroutine 
	def ensure_path(self, path, acl=None):
		event = self.client.ensure_path_async(path, acl)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	
	@tornado.gen.coroutine 
	def exists(self, path):
		event = self.client.exists_async(path)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	@tornado.gen.coroutine 
	def get(self, path):
		event = self.client.get_async(path)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	
	@tornado.gen.coroutine 
	def get_acls(self, path):
		event = self.client.get_acls_async(path)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	
	@tornado.gen.coroutine 
	def set_acls(self, path, acls, version):
		event = self.client.set_acls_async(path, acls, version)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	@tornado.gen.coroutine 
	def set(self, path, value, version):
		event = self.client.set_async(path, value, version)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())
	
	@tornado.gen.coroutine 
	def delete(self, path, version=-1):
		event = self.client.delete_async(path, version)
		yield tornado.gen.Task(event.rawlink)
		raise tornado.gen.Return(event.get_nowait())	
	
	
	####################future###########################
	#future change 
	def get_future(self, path):
		future = Future()
		self.client.get_async(path, lambda data = None:future.set_result(data))
# 		yield tornado.gen.Task(event.rawlink)
		return future
	

	def exist_future(self, path):
		future = Future()
		self.client.exist_async(path, lambda data = None:future.set_result(data))
# 		yield tornado.gen.Task(event.rawlink)
		return future

	def get_child_future(self, path):	
		future = Future()
		self.client.get_children_async(path, lambda data = None:future.set_result(data))
# 		yield tornado.gen.Task(event.rawlink)
		return future

count = 0

@tornado.gen.coroutine 
def work(tt, path):
	while True:
		try:
			result = yield tt.get_child_future(path)
			print(result)
		except Exception, e:
			print(e)
			
@tornado.gen.coroutine 
def print_count():
	global count 
	while True:
		yield tornado.gen.sleep(1)
		print(count)
		count = 0
	

@tornado.gen.coroutine 
def fun():
	try:
		tt = test()
		yield tt.start(10)
		yield work(tt, '/t')
		yield print_count()
	except:
		pass

if __name__ == '__main__':
	loop = tornado.ioloop.IOLoop.instance()
	f = fun()
	print(type(f))
	loop.add_future(f, lambda f:f.result())
	loop.start()
