import json
import numpy as np
import pandas as pd
import sys
from readGroundTruth import groundTruth
import statsmodels.stats.api as sms


def findIndexTime(df):

    times = pd.to_datetime(df.time, unit='s')

    startTime = times.iloc[0].replace(second=0, microsecond=0)
    endTime = times.iloc[-1].replace(second=0, microsecond=0)
    indexTime = pd.date_range(start=startTime, end=endTime, freq='5s')

    return indexTime
