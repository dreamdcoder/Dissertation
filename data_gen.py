""" data_gen.py for generating data for streaming purpose"""
from datetime import datetime
import json

class DataGen:
    def __init__(self, key):
        """
        :param key:
        initialize data_gen object for a particular node
        """
        self.key = key

    def prepare_record(self):
        """
        generates data in json format
        :return:
        """
        active_routes_count = 11.0
        backup_routes_count = 0.0
        deleted_routes_count = 9.0
        paths_count = 1282.0
        protocol_route_memory = 168160.0
        total_neighbors_count = 0.0
        performance_stat_global_config_items_processed = 0.0
        performance_stat_vrf_inbound_update_messages = 106484.0
        vrf_path_count = 6140.0
        vrf_update_messages_received = 106484.0
        event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return {"key": self.key, "time": event_time, "active_routes_count": active_routes_count,
                "backup_routes_count": backup_routes_count, "deleted_routes_count": deleted_routes_count,
                "paths_count": paths_count,
                "performance_stat_global_config_items_processed": performance_stat_global_config_items_processed,
                "performance_stat_vrf_inbound_update_messages": performance_stat_vrf_inbound_update_messages,
                "protocol_route_memory": protocol_route_memory, "total_neighbors_count": total_neighbors_count,
                "vrf_path_count": vrf_path_count, "vrf_update_messages_received": vrf_update_messages_received}
