import argparse
import os
import datetime
from configobj import ConfigObj
import sys
from clickhouse_driver import Client
from rabbitmq import RabbitConnection
import json


class ClickHouseSaver:
    def __init__(self):
        self.queue = None
        self.client = Client(host='bigdata1-chel2', port='3306')

    def from_rabbit_to_clickhouse(self, config):
        self.queue = RabbitConnection(config)
        self.queue.read_queue(self.read_from_rabbit, 'perIface2Mongo')

    def read_from_rabbit(self, channel, method_frame, header_frame, body):
        data = json.loads(body.decode())
        for row in data:
            self.parse_row(row)

    def get_high_low_bits(self, num):
        binary = bin(num)[2:].zfill(64)
        high_bits = binary[:32]
        low_bits = binary[32:]
        return int(high_bits, 2), int(low_bits, 2)

    def parse_row(self, data):
        agent_ip, agent_interface_id = self.get_high_low_bits(data[0])
        ip, network_mask_prefix = self.get_high_low_bits(data[1])
        num_threads = data[2]
        num_packets = data[3]
        num_bytes = data[4]
        bandwidth = data[5]
        num_events = data[6]
        self.save_to_clickhouse(agent_ip, agent_interface_id, ip, network_mask_prefix, num_threads, num_packets,
                                num_bytes, bandwidth, num_events)

    @staticmethod
    def get_network_address(ip, prefix):
        mask = 0
        for i in range(32):
            if i < prefix:
                mask |= 1 << i
            else:
                mask <<= 1
        return ip & mask

    def save_to_clickhouse(self, agent_ip, agent_interface_id, ip, network_mask_prefix, num_threads, num_packets,
                           num_bytes, bandwidth, num_events):
        net_ip = self.get_network_address(ip, network_mask_prefix)

        self.client.execute('insert into netflow.netflow_statistics_buffer values',
                            [[datetime.date.today(), datetime.datetime.now(), agent_ip, agent_interface_id, ip,
                              network_mask_prefix, net_ip, num_threads, num_packets, num_bytes, bandwidth, num_events]])

        # print(datetime.datetime.now())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Video stream capturing component for face detection and recognition service')
    parser.add_argument('--config', '-c',
                        dest='config', type=str,
                        default="/home/clara/PycharmProjects/netflow_clickhouse/config.cfg",
                        help='Path to configuration file'
                        )
    args = parser.parse_args()

    if not os.path.exists(args.config):
        sys.exit("No such file or directory: %s" % args.config)

    config = ConfigObj(args.config)
    for config_section in ('rabbitmq',):
        if config_section not in config:
            sys.exit("Mandatory section missing: %s" % config_section)

    saver = ClickHouseSaver()
    saver.from_rabbit_to_clickhouse(args.config)
