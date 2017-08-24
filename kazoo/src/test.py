
import sys 
import os
sys.path.append(os.getcwd())

from myhandler import MyHandler, get_child_state_watcher, get_child_data_watcher
import tornado.ioloop
import tornado.gen


@tornado.gen.coroutine 
def op_zookeeper():
    zk = MyHandler.get_zookeeper()

    result = yield zk.ensure_path_async('/test').get_future()
    print(result)
    result = yield get_child_state_watcher(zk, '/test')
    print(result)
    
    result = yield get_child_data_watcher(zk, '/test/a')
    print(result)
    
if __name__ == '__main__':
    MyHandler.get_zookeeper(hosts = '192.168.8.136:2181')
    loop = tornado.ioloop.IOLoop.instance()
    loop.run_sync(op_zookeeper)
    
