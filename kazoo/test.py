
import sys 
import os
sys.path.append(os.getcwd())

from myhandler import MyHandler
import tornado.ioloop
import tornado.gen

count = 0

@tornado.gen.coroutine 
def print_count():
    while True:
        global count
        print(count)
        count = 0
        yield tornado.gen.sleep(1)    

@tornado.gen.coroutine 
def op_zookeeper():
    future = print_count()
    tornado.ioloop.IOLoop.instance().add_future(future, lambda future: future.result())
    while True:
        global count
        count = count + 1
        handler = MyHandler(hosts='192.168.8.147:2181')
        zk = yield handler.get_zookeeper(10)
        yield zk.delete_async('/test/c').get_future()
        result = yield zk.create_async('/test/c', b'yangmeng', ephemeral = True).get_future()

        print(result)
        zk.stop()
    
if __name__ == '__main__':
    loop = tornado.ioloop.IOLoop.instance()
    loop.run_sync(op_zookeeper)
    
