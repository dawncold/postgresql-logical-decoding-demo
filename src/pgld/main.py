import json
import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection
import pika


class PurchaseService(object):
    def __init__(self):
        self._slot_name = 'test_slot'
        self._db_conn = psycopg2.connect('host=localhost dbname=test_db user=dc',
                                         connection_factory=LogicalReplicationConnection)
        self._cur = self._db_conn.cursor()
        self._mq_conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._mq_channel = self._mq_conn.channel()
        self.init()

    def init(self):
        self._create_logical_replication_slot(self._slot_name)
        self._mq_channel.queue_declare(queue='purchase')

    def _create_logical_replication_slot(self, name):
        try:
            self._cur.create_replication_slot(name, slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                              output_plugin='wal2json')
        except psycopg2.ProgrammingError as err:
            if err.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
                raise
            else:
                print('slot already exists')

    def start(self):
        self._cur.start_replication(slot_name=self._slot_name, decode=True)
        self._cur.consume_stream(self._process_message)

    def _process_message(self, msg):
        obj = json.loads(msg.payload)
        for e in obj['change']:
            kind = e['kind']
            if kind != 'insert':
                continue
            table = '{}.{}'.format(e['schema'], e['table'])
            row = dict(zip(e['columnnames'], e['columnvalues']))
            print(f'enqueue a job with data: [{kind}] table {table}, row obj {row}')
            self._mq_channel.basic_publish(exchange='', routing_key='purchase', body=json.dumps(row))
        msg.cursor.send_feedback(flush_lsn=msg.data_start)


if __name__ == '__main__':
    PurchaseService().start()
