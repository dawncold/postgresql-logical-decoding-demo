import json
import pika
import psycopg2


class CreditCardService(object):
    def __init__(self):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._channel = self._conn.channel()
        self.init()

    def init(self):
        self._channel.queue_declare(queue='purchase')
        self._channel.basic_consume(self.callback, queue='purchase', no_ack=True)

    def callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
        obj = json.loads(body)

        purchase_id = obj['id']
        with psycopg2.connect('host=localhost dbname=test_db user=dc') as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute('UPDATE purchase SET paid_at=CURRENT_TIMESTAMP WHERE id=%(id)s', {'id': purchase_id})

    def start(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self._channel.start_consuming()


if __name__ == '__main__':
    CreditCardService().start()
