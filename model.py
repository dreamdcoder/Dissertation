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
import joblib

# Set Pandas Dataframe Display Options
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# instantiate data object
d = data()
df = d.read_data('leaf1')

# print(df)

df_new = df.drop(['node_name', 'time'], axis=1)

scaler = MinMaxScaler()

scaler.fit(df_new)
scaled_df = scaler.transform(df_new)
file_name="leaf1_scaler.sav"
joblib.dump(scaler,file_name)
#dump(scaler, open("leaf1_scaler.pkl", 'wb'))
print(scaled_df)
