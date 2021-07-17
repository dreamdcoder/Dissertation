""" data_gen.py for generating data for streaming purpose"""
from datetime import datetime
import json
import random

class DataGen:
    def __init__(self, key):
        """
        :param key:
        initialize data_gen object for a particular node
        """
        self.key = key
        config = json.load(open('config.json'))
        self.active_routes_count_list = config[self.key]['active_routes_count']
        self.backup_routes_count_list = config[self.key]['backup_routes_count']
        self.deleted_routes_count_list = config[self.key]['deleted_routes_count']
        self.paths_count_list = config[self.key]['paths_count']
        self.protocol_route_memory_list = config[self.key]['protocol_route_memory']
        self.total_neighbors_count_list = config[self.key]['total_neighbors_count']
        self.perf_stat_glob_conf_items_processed_list = config[self.key]['performance_stat_global_config_items_processed']
        self.perf_stat_vrf_inb_updt_msg_list = config[self.key]['performance_stat_vrf_inbound_update_messages']
        self.vrf_path_count_list = config[self.key]['vrf_path_count']
        self.vrf_update_messages_received_list = config[self.key]['vrf_update_messages_received']

    def prepare_record(self):
        """
        generates data in json format
        :return:
        """
        
        active_routes_count = random.choice(self.active_routes_count_list)
        backup_routes_count = random.choice(self.backup_routes_count_list)
        deleted_routes_count = random.choice(self.deleted_routes_count_list)
        paths_count = random.choice(self.paths_count_list)
        protocol_route_memory = random.choice(self.protocol_route_memory_list)
        total_neighbors_count = random.choice(self.total_neighbors_count_list)
        performance_stat_global_config_items_processed = random.choice(self.perf_stat_glob_conf_items_processed_list)
        performance_stat_vrf_inbound_update_messages = random.choice(self.perf_stat_vrf_inb_updt_msg_list)
        vrf_path_count = random.choice(self.vrf_path_count_list)
        vrf_update_messages_received = random.choice(self.vrf_update_messages_received_list)
        event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        return {"key": self.key, "time": event_time, "active_routes_count": active_routes_count,
                "backup_routes_count": backup_routes_count, "deleted_routes_count": deleted_routes_count,
                "paths_count": paths_count,
                "performance_stat_global_config_items_processed": performance_stat_global_config_items_processed,
                "performance_stat_vrf_inbound_update_messages": performance_stat_vrf_inbound_update_messages,
                "protocol_route_memory": protocol_route_memory, "total_neighbors_count": total_neighbors_count,
                "vrf_path_count": vrf_path_count, "vrf_update_messages_received": vrf_update_messages_received}
