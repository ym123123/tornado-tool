from myhandler import MyHandler
import tornado.ioloop
import tornado.gen

@tornado.gen.coroutine 
def op_zookeeper():
    zk = MyHandler.get_zookeeper(hosts = '127.0.0.1:2181')
    
    result = yield zk.get_async('/test')
    print(result)
    result = yield zk.set_async('/test', b'yangmeng1234')
    print(result)
    result = yield zk.get_async('/test')
    print(result)
    

if __name__ == '__main__':
    loop = tornado.ioloop.IOLoop.instance()
    loop.run_sync(op_zookeeper)
    
