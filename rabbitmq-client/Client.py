import pika 

if __name__ == '__main__':
    conn = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
    ch = conn.channel()
    ch.queue_declare('t1', auto_delete = True)
    ch.exchange_declare(exchange = 'tx', auto_delete = True)
    ch.queue_bind(queue = 't1', exchange = 'tx')
    
    def on_consumer(ch, data1, data2, data3):
        print(data1)
        print(ch)
        print(data2)
        print(data3)
    
    ch.basic_consume(on_consumer, queue = 't1')
    ch.start_consuming()
    ch.close()
    conn.close()