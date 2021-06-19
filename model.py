"""
model.py creates model for machine learning
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
import seaborn as sns
import matplotlib.dates as mdates
from sklearn.cluster import DBSCAN
from sklearn import metrics
from cassandra_data_mgmt import data
from pickle import dump
from sklearn.ensemble import IsolationForest
import joblib
import os


def create_model(node_name):
    '''
    creates and saves pickle files for model and scaler
    :param node_name:
    :return:
    '''
    path = os.getcwd() + '\model'
    # Set Pandas Dataframe Display Options
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    # instantiate data object

    d = data()
    # data ingestion
    df = d.read_data(node_name)
    # print(df)

    # data preprocessing
    df_new = df.drop(['node_name', 'time'], axis=1)

    # data normalization
    scaler = MinMaxScaler()
    scaler.fit(df_new)
    scaled_df = scaler.transform(df_new)

    #create a pickle file for scaler
    scaler_file_name = node_name + "_scaler.pkl"

    spath = os.path.join(path, scaler_file_name)
    with open(spath, 'wb') as f:
        dump(scaler, f)

    #create model
    ilf = IsolationForest(n_estimators=100, contamination=0.01)
    ilf.fit(scaled_df)
    #create a pickle file for scaler
    model_file_name = node_name + "_model.pkl"
    mpath = os.path.join(path, model_file_name)
    with open(mpath, 'wb') as f:
        dump(ilf, f)


if __name__ == "__main__":
    d = data()
    nodes = d.get_nodes()
    for node in nodes:
        create_model(node)
        print("model created for {}".format(node))