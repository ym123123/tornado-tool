import tornado.gen 
import socket
from tornado.iostream import IOStream

class base(IOStream):
    __bases = {}
    def __init__(self, sockfd, *args, **kargs):
        super(base, self).__init__(sockfd, *args, **kargs)
        self.set_close_callback(self.__close_base)
        self.fd = sockfd
        base.__bases[self.fd] = self
        
    def __close_base(self):
        del base.__bases[self.fd]
        
        
    @tornado.gen.coroutine
    def __work_handler(self):    
        try:
            while True:
                ret = yield self.read_bytes(20)
                print(ret)
        except:
            tornado.gen.Return('clsoe fd')
    def work_handler(self):
        future = self.__work_handler()
        loop.add_future(future, lambda future: future.result())
        
    @staticmethod
    def accept_handler(sfd, events):
        cfd, addr = sfd.accept()
        mybase = base(cfd)
        mybase.work_handler()
        print('=========end==========')
        
    
if __name__ == '__main__':
    loop = tornado.ioloop.IOLoop.instance()
    sfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    
    sfd.bind(('0.0.0.0', 80))
    sfd.listen(10)
    print('================')
    loop.add_handler(sfd, base.accept_handler, loop.READ)
    loop.start()