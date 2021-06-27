'''my_syn_gen.py'''
import os
import sys
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone, date
import influxdb, influxdb.exceptions

# Global Constant variable for holding config file name

CONFIG_FILE = 'my_syn_gen.conf'
# dictionary object
CONFIG = dict()

TIME_FORMAT = '%Y-%m-%d %H:%M:%S.03d %z'


def read_config(conf_file):
    global CONFIG
    with open(conf_file) as config:
        CONFIG = json.load(config)
        print(config)
    CONFIG = mode_translate(CONFIG)

    return CONFIG


def mode_translate(config):
    for k in list(config.keys()):
        print(k)


if __name__ == '__main__':
    CONFIG_FILE = r"C:\Users\yogeshja\Desktop\Dissertation\syn_gen.conf"
    config = read_config(CONFIG_FILE)
    print(config)
