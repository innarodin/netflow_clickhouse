import argparse
import os
from configobj import ConfigObj
import sys
from clickhouse_driver import Client
from rabbitmq import RabbitConnection
import json


class ClickHouseSaver:
    def read_from_rabbit(self, channel, method_frame, header_frame, body):
        data = json.loads(body.decode())
        for row in data:
            print(row)
            self.parse_row(row)

    def get_high_low_bits(self, num):
        binary = bin(num)[2:].zfill(64)
        half_len = int((len(binary) / 2))

        high_bits = binary[: half_len]
        low_bits = binary[half_len:]

        return int(high_bits, 2), int(low_bits, 2)

    def parse_row(self, data):
        agent_ip, agent_interface_id = self.get_high_low_bits(data[0])
        ip, network_mask_prefix = self.get_high_low_bits(data[1])
        num_threads = data[2]
        num_packets = data[3]
        num_bytes = data[4]
        bandwidth = data[5]
        num_events = data[6]
        print(agent_ip, agent_interface_id, ip, network_mask_prefix, num_threads, num_packets, num_bytes, bandwidth,
              num_events)

        self.get_network_address(ip, network_mask_prefix)

        self.save_to_clickhouse(agent_ip, agent_interface_id, ip, network_mask_prefix, num_threads, num_packets,
                                num_bytes, bandwidth, num_events)

    def save_to_clickhouse(self, agent_ip, agent_interface_id, ip, network_mask_prefix, num_threads, num_packets,
                           num_bytes, bandwidth, num_events):
        net_ip = self.get_network_address(ip, network_mask_prefix)

        client = Client('localhost')
        client.execute('insert into netflow_stat values', [[agent_ip, agent_interface_id, ip, network_mask_prefix,
                                                            net_ip, num_threads, num_packets, num_bytes,
                                                            bandwidth, num_events]])

    def get_network_address(self, ip, prefix):
        bin_ip = bin(ip)[2:].zfill(32)
        bin_net = bin_ip[:prefix].ljust(32, '0')
        return int(bin_net, 2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Video stream capturing component for face detection and recognition service')

    parser.add_argument('--config', '-c',
                        dest='config', type=str,
                        default="config.cfg",
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

    queue = RabbitConnection(args.config)
    queue.read_queue(saver.read_from_rabbit, 'perIface2Mongo')
