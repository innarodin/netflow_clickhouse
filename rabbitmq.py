import random
from configobj import ConfigObj
import pika
import sys


class RabbitConnection:
    def __init__(self, path_config):
        self.queue_name = None
        self.config = ConfigObj(path_config)

        amqp_url = 'amqp://%s:%s@%s:%s/%s' % (
            self.config['rabbitmq']['user'],
            self.config['rabbitmq']['pass'],
            self.config['rabbitmq']['host'],
            self.config['rabbitmq']['port'],
            self.config['rabbitmq']['vhost'],
        )
        amqp_url_query = {
            'heartbeat_interval': 600
        }

        self.exchange = self.config['rabbitmq']['exchange']
        self.amqp_parameters = pika.URLParameters(
            amqp_url + '?' + '&'.join(['%s=%s' % (k, v) for k, v in amqp_url_query.items()]))

        self.create_connection()

    def create_connection(self):
        try:
            self.connection = pika.BlockingConnection(self.amqp_parameters)
            self.channel = self.connection.channel()
        except pika.exceptions.AMQPConnectionError as err:
            msg = {
                "service_id": None,
                "session_id": None,
                "status": "Pika exception: {}".format(err)
            }
            print(msg)
            sys.exit(255)

    def read_queue(self, callback, name_queue):
        self.queue_name = name_queue
        self.channel.queue_bind(exchange=self.exchange,
                                queue=self.queue_name,
                                routing_key=self.queue_name)

        consumer_id = ''.join(['%02X' % random.getrandbits(8) for _ in range(8)])

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
        lambda channel, method_frame, header_frame, body: callback(channel, method_frame, header_frame, body),
            queue=self.queue_name, consumer_tag='{}.{}'.format(self.queue_name, consumer_id), no_ack=True)

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        except pika.exceptions.ConnectionClosed as err:
            msg = {
                "service_id": None,
                "session_id": None,
                "status": "RabbitMQ connection closed. Reason: {}".format(err)
            }
            print(msg)
            sys.exit(255)
