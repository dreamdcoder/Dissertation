"""
cassandra_data_mgmt.py for managing cassandra opertaions
"""

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from cassandra.query import SimpleStatement
import pandas as pd
import os
import glob2


class data:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'], connect_timeout=9999)
        self.session = self.cluster.connect()
        self.keyspace = 'dissertation'
        self.create_keyspace()
        self.session.set_keyspace(self.keyspace)

    def create_keyspace(self):
        self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
         """ % self.keyspace)

    def insert_record(self, row):

        query = SimpleStatement(
            """INSERT INTO dataplane(time,active_routes_count,backup_routes_count,total_neighbors_count,deleted_routes_count,
        paths_count, protocol_route_memory, performance_stat_global_config_items_processed,
        performance_stat_vrf_inbound_update_messages, vrf_path_count, vrf_update_messages_received,
        node_name)VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""")
        print(query)
        self.session.execute(query, row)

    def initial_transfer(self):
        self.session.set_keyspace(self.keyspace)
        query = """CREATE TABLE IF NOT EXISTS dataplane
                    (time timestamp,
                     active_routes_count double,
                     backup_routes_count double,
                     total_neighbors_count double,
                     deleted_routes_count double,
                     paths_count double,
                     protocol_route_memory double,
                     performance_stat_global_config_items_processed double,
                     performance_stat_vrf_inbound_update_messages double,
                     vrf_path_count double,
                     vrf_update_messages_received double,
                     node_name text,
                     PRIMARY KEY (node_name, time))
                     WITH CLUSTERING ORDER BY (time DESC);"""
        self.session.execute(query)
        featureList = ['time', 'active-routes-count', 'backup-routes-count',
                       'global__established-neighbors-count-total',
                       'deleted-routes-count', 'paths-count', 'protocol-route-memory',
                       'performance-statistics__global__configuration-items-processed',
                       'performance-statistics__vrf__inbound-update-messages', 'vrf__path-count',
                       'vrf__update-messages-received']

        # get path of data sets
        path = os.getcwd() + '\data'

        # returns all file having pattern from a path provided
        csv_files = glob2.glob(os.path.join(path, "*.csv"))

        # for loop to prepare pd data frames and processing them
        for f in csv_files:
            file_name = f.split('\\')[-1]
            node_name = file_name.split('_')[0].replace('bgpclear', '')
            print(node_name)
            df = pd.read_csv(f).dropna().drop('Unnamed: 0', axis=1)
            df = df[featureList]  # select only dataplane features
            df['time'] = pd.to_datetime(df['time'], unit='ms').astype(str)
            df['node_name'] = node_name  # add extracted node_name in node_name column
            # rename pandas data frame columns
            df = df.rename({'global__established-neighbors-count-total': 'total-neighbors-count',
                            'performance-statistics__global__configuration-items-processed': 'performance-stat-global-config-items-processed',
                            'performance-statistics__vrf__inbound-update-messages': 'performance-stat_vrf_inbound-update-messages',
                            'vrf__update-messages-received': 'vrf_update-messages-received'}, axis=1)
            rows = [tuple(x) for x in df.values]
            for row in rows:
                self.insert_record(row)


    def pandas_factory(self, colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    def close_session(self):
        self.session.shutdown()

    def get_nodes(self):
        self.session.row_factory = self.pandas_factory
        self.session.default_fetch_size = None
        query = """select distinct node_name from dataplane;"""
        # print(query)
        rslt = self.session.execute(query, timeout=None)
        df = rslt._current_rows
        node_list=list(df.iloc[:,0])
        return node_list

    def read_data(self, node_name):
        self.session.row_factory = self.pandas_factory
        self.session.default_fetch_size = None
        query = """select * from dataplane where node_name='{}';""".format(node_name)
        #print(query)
        rslt = self.session.execute(query, timeout=None)
        df = rslt._current_rows
        return df