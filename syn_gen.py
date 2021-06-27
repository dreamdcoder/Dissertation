import os
import sys
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone, date
import influxdb, influxdb.exceptions
#import logging

os.environ


CONFIG_FILENAME = 'synthetic_generator.conf'
forked_mode = None
CONF = dict()

# Define a noise distribution for needles, where we control the percentage occurrence (1%) of the maximum value (1.0)
NEEDLES_NOISE_DISTRO = np.exp(-0.2 * np.arange(0, 100))
MAX_WRITE_BATCH_SIZE = 20000  # upper limit on number of points to batch for a single write operation to the influxDB
                            # client API

STRFTIME_FORMAT_FOR_LOG_TIMESTAMP = '%Y-%m-%d %H:%M:%S.%03d %z'

'''def place_stop(name=""):
    """ since it is hard to stop the script (Ctrl-C on all threads), we will place a stop script before we start """
    with open("/stop"+name,'w') as f:f.write("#!/bin/bash\necho $(kill -9 "+str(os.getpid())+')\necho Stopped.\nrm -- \"$0\"\n')
    print("To kill run $bash /stop"+name+" or kill -9 "+str(os.getpid()))'''

def mode_translate(CONF,mode):
    """
    an utility func to apply conf according to mode:
    all that start with R will be translated only during Realtime mode, and vise versa
    """
    def translate(k):
        if k.startswith('R') and mode=='realtime':
            print("CONF."+mode+".translated: "+k[1:])
            return k[1:]
        if k.startswith('B') and mode=='batch':
            print("CONF."+mode+".translated: "+k[1:])
            return k[1:]
        return k
    # only dicts and lists [holding int,strings] allowed in CONF
    if type(CONF) is dict:
        for k in list(CONF.keys()):
            CONF[translate(k)]=mode_translate(CONF[k],mode)
    if type(CONF) is list:
        for i in range(len(CONF)):
            CONF[i]=mode_translate(CONF[i],mode)
    return CONF

def load_config(CONFIG_FILENAME=CONFIG_FILENAME,mode=None):
    """ read config file to global var CONF """
    global CONF
    with open(CONFIG_FILENAME) as f:
        CONF = json.load(f)
        #exec("CONF="+f.read())
    if mode:
        print(level=getattr(CONF['jobs'].get("loglevel","INFO")), format='%(asctime)s [%(levelname)s] '+str(mode)+'<%(thread)d|%(threadName)s> %(message)s',
                    datefmt=STRFTIME_FORMAT_FOR_LOG_TIMESTAMP,stream=sys.stdout)
    CONF=mode_translate(CONF,mode=mode or CONF['jobs']['mode'])
    return CONF


def sizeof_fmt(num, suffix='B'):
    """ helper func for bytes to human """
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def datetime_parser(datetime_str):
    datetime_str = str(datetime_str)
    if True:
        try:
            if len(datetime_str) == 8:
                # Assume format YYYYMMDD
                datetime_obj = datetime(int(datetime_str[0:4]),
                                          int(datetime_str[4:6]),
                                          int(datetime_str[6:8]),
                                          tzinfo=timezone.utc)
            elif len(datetime_str) == 12:
                # Assume format YYYYMMDDhhmm
                datetime_obj = datetime(int(datetime_str[0:4]),
                                          int(datetime_str[4:6]),
                                          int(datetime_str[6:8]),
                                          int(datetime_str[8:10]),
                                          int(datetime_str[10:12]),
                                          tzinfo=timezone.utc)
            else:
                raise ValueError

        except ValueError:
            print("ERROR: date-time strings must have format yyyymmdd[HHMM]. Received: {} .".format(datetime_str),
                  file=sys.stderr)
            exit(1)
    return datetime_obj


to_midnight=lambda t:t-(datetime.combine(date.min,t.time()) - datetime.min)


def rangetime(f:datetime,t:datetime,noyield=False):
    "returns a iterable where each element is (start,end) timestamp tuple breaking down the main tuple(f,t) datewise"
    fe=to_midnight(f)+timedelta(1)
    ts=to_midnight(t)
    if noyield:
        return list(rangetime(f,t,False))
    else:
        if fe>ts:
            if fe<=t:
                raise NotImplemented("Unexpected")
            else:
                yield tuple(pd.date_range(f,t,freq=f"{int((t-f).total_seconds())}S"))
        else:
            yield tuple(pd.date_range(f,fe,freq=f"{int((fe-f).total_seconds())}S"))
            for rs in zip(pd.date_range(fe,ts),pd.date_range(fe,ts)[1:]):  # freq=1D
                yield rs
            if int((t-ts).total_seconds()):
                yield tuple(pd.date_range(ts,t,freq=f"{int((t-ts).total_seconds())}S"))


def rangetimedelta_custom(start,stop,step):
    cur, curend = start, start+step
    index=0
    while cur<=stop:
        yield cur-to_midnight(start),min(curend,stop)-to_midnight(start),index
        cur,curend,index=curend,curend+step,index+1


def generate_clean_series_from_base_pattern(
        base_pattern_values: list,
        target_interval_secs: float,
        model: str
) -> pd.Series:
    """
    Generate a 24-hour Series from a list of values, upsampled to the desired frequency.

    Parameters
    ----------
    base_pattern_values
    target_interval_secs
    model

    Returns
    -------

    """

    # Generate a Pandas Series which fits the input values list spread evenly across 24 hours
    # The generated index goes from 00:00 to 24:00
    base_pattern = pd.Series(
        data=base_pattern_values + [base_pattern_values[-1]],
        index=pd.timedelta_range(start=0, end='1 days', periods=len(base_pattern_values) + 1)
    )

    # Generate the target time-of-day points for the desired frequency
    target_timedeltas = pd.timedelta_range(start='0 days',
                                           end='1 days',
                                           freq=f'{int(target_interval_secs*1000*1000*1000)}N')

    # Upsample the user data to the desired frequency
    if model.lower() in ['daily_patterns', 'util_cpu', 'util_mem', 'util_fs']:
        # -- Upsample with linear interpolation

        upsampled = pd.Series(index=target_timedeltas,dtype=np.float64).append(base_pattern).sort_index().interpolate(method='time')
        # keep only the target indexes (maybe they don't overlap with the base pattern indexes)
        upsampled = upsampled.loc[target_timedeltas]
        # remove common indexes coming from both target and base pattern
        upsampled = upsampled[~upsampled.index.duplicated(keep='last')]
        # remove 24:00 point
        upsampled = upsampled[:-1]

    elif model.lower() in ['needles', 'binary']:
        # Upsample using 'last value' interpolation
        upsampled = base_pattern.resample(f"{target_interval_secs}S", label='right', closed='right').ffill()[:-1]

    else:
        print(f"Unknown model: {model}", file=sys.stderr)
        exit(1)

    return upsampled


def add_noise(
        base_pattern: pd.Series,
        additive_uniform_noise: float = 0.0,
        additive_needles_noise: float = 0.0,
        multiplicative_noise : float = 1.0,
        random_generator=np.random.RandomState()
)-> pd.Series:
    """
    Add white noise.

    Parameters
    ----------
    base_pattern : pd.Series
        The base time series to add noise to.
    additive_uniform_noise : float
        The maximum positive deviation to add as noise
    additive_needles_noise : float
        The maximum positive deviation to add as noise
    multiplicative_noise : float
        The maximum value to multiply the base pattern value by
    random_generator
        A numpy random number generator, e.g. np.random.default_rnd()

    Returns
    -------
    pd.Series
        A new time series

    """

    noisy_pattern = base_pattern#.copy()
    scale_values = random_generator.uniform(1,multiplicative_noise,len(base_pattern))
    noisy_pattern = noisy_pattern * scale_values
    if additive_uniform_noise > 0.0:
        noisy_pattern = noisy_pattern + random_generator.uniform(0.0, additive_uniform_noise, len(base_pattern))
    if additive_needles_noise > 0.0:
        noisy_pattern = noisy_pattern + additive_needles_noise*np.vectorize(lambda x: NEEDLES_NOISE_DISTRO[x])\
        (random_generator.randint(0, len(NEEDLES_NOISE_DISTRO), len(base_pattern)))
    return noisy_pattern


def add_anomalies(
        series_in: pd.Series,
        start_offset: pd.Timedelta,
        anomaly_interval: pd.Timedelta,
        anomaly_duration: pd.Timedelta,
        anomaly_value_delta: float,
        anomaly_value_multiplicative: float,
        random_generator=np.random.RandomState()
) -> pd.Series:
    """
    Add anomalies to a prepared time series.

    Returns
    -------

    """
    # At regular time intervals of 'anomaly_interval', starting from 00:00 + 'start_offset',
    # for a duration of 'anomaly_duration', alter the series values as follows:
    # (maintaining the series_in original index)
    # 1. multiply by 'anomaly_value_relative' the series_in value
    # 2. add 'anomaly_value_delta' to the series value
    # Handle Duplicates - having same anomalies.
    # anomaly_interval >> anomaly_duration
    ...
    series_out = series_in#.copy()
    assert anomaly_interval > anomaly_duration, "NotImplemented other scenarios"
    worksets=pd.timedelta_range(start=start_offset,end='1 days',freq=anomaly_interval)
    ss=0
    for start,end in zip(worksets,worksets[1:]):
        # we need to choose a random time-window `anomaly_duration` for creating anomalies inside start-end window
        window_picker_size=(end-start-anomaly_duration).total_seconds()
        anom_start=start+pd.to_timedelta(random_generator.randint(window_picker_size),unit='s')
        anom_end=anom_start+anomaly_duration+pd.Timedelta(1, unit='ns')
        assert anom_end<=end, "Unexpected: Locha ee ulfat ho gaya"
        series_out[anom_start:anom_end]=series_out[anom_start:anom_end]*anomaly_value_multiplicative+anomaly_value_delta
        ss+=len(series_out[anom_start:anom_end])
    # print("Anomalous Data points converted:",ss,"/",len(series_out))
    return series_out


def add_jitter(
        pattern: pd.Series,
        jitter_max_timedelata: pd.Timedelta,
        random_generator=np.random.RandomState()
) -> pd.Series:
    """
    Add jitter to the series index

    Parameters
    ----------
    pattern : pd.Series
        The time series to add jitter to
    jitter_max_timedelata : pd.Timedeltra
        The max amount of random delay to add

    Returns
    -------
    pd.Series
        Original time series with jitter added.

    """
    #deltas = np.concatenate([random_generator.uniform(0, jitter_max_timedelata.total_seconds(), len(pattern)-1),[0]])
    deltas = random_generator.uniform(0, jitter_max_timedelata.total_seconds(), len(pattern))
    deltas = pd.to_timedelta(deltas, unit='s')
    pattern_w_jitter = pattern#.copy()
    pattern_w_jitter.index = pattern.index + deltas
    return pattern_w_jitter


'''
org_rand_state=np.random.RandomState
def RandomState(*args,_cache_dict=dict(),**kwargs):
    from functools import lru_cache
    if len(args)+len(kwargs)==0:
        return org_rand_state()
    k=(args,kwargs)
    if k in _cache_dict:return _cache_dict[k]
    rd=org_rand_state(*args,**kwargs)
    _cache_dict[k]=rd
    rd.uniform=lru_cache(rd.uniform,maxsize=128)
'''


def add_offset(
        pattern: pd.Series,
        offset_timedelta: pd.Timedelta
) -> pd.Series:
    """
    Add an offset to the series index

    Parameters
    ----------
    pattern : pd.Series
        The time series to add offset to
    offset_timedelta : pd.Timedelta
        The amount of offset to add

    Returns
    -------
    pd.Series
        Original time series with offset added.

    """
    pattern_w_offset = pattern#.copy()
    pattern_w_offset.index = pattern.index + offset_timedelta
    return pattern_w_offset


def generate_all_series_for_one_day(day=0, start:pd.Timedelta=None, end:pd.Timedelta=None, Index:int=None) -> pd.DataFrame:
    """
    Generate time series for all metrics in a single DataFrame, including noise and jitter, for a single day.

    Returns
    -------
    pd.DataFrame
        index is timestamp
        columns are field value and tag values
    """
    shuru=datetime.utcnow()
    series_to_df=lambda s,tags:pd.concat([s.to_frame(name="VALUE"), pd.DataFrame(tags,index=s.index)], axis=1)
    finaldfs=[]
    #todo : SSLY REDUCE FOR LOOPS
    for gen_pattern in CONF['generated_patterns']:
        if 'clean_series' not in gen_pattern:
            continue
        cleanS=gen_pattern['clean_series']
        if start!=None and end!=None:
            cleanS=cleanS[start:end-pd.Timedelta(1, unit='ns')]  # [inclusive:exclusive]
        if not len(cleanS):
            continue
        for dup_id in range(gen_pattern['duplication_count']):
            tags=dict(gen_pattern['tags'])
            tags[gen_pattern['duplication_tag']]+="__{}__{}".format(CONF['jobs']['duplication_name'],dup_id+1)
            S=cleanS.copy()
            noise_seed=gen_pattern.get('white_noise_seed', gen_pattern.get('needles_noise_seed',None))
            if noise_seed!=None: # different seeds for different duplicates, different days, different Index
                noise_seed+=dup_id*(dup_id+1)*(day+1)**3+(Index or 0)
            if not dup_id and gen_pattern['anomaly']['enable']:
                S=add_anomalies(S,
                                start_offset=pd.to_timedelta(gen_pattern['anomaly']['offset_minutes'],unit='m'),
                                anomaly_interval=pd.to_timedelta(gen_pattern['anomaly']['interval_minutes'],unit='m'),
                                anomaly_duration=pd.to_timedelta(gen_pattern['anomaly']['duration_points'],unit='m'),
                                anomaly_value_delta=gen_pattern['anomaly']['delta_absolute'],
                                anomaly_value_multiplicative=gen_pattern['anomaly']['delta_relative'],
                                random_generator=np.random.RandomState(noise_seed)
                               )
            S=add_noise(S, gen_pattern.get('white_noise_max',0), gen_pattern.get('needles_noise_max',0),
                        multiplicative_noise=gen_pattern.get('noise_relative',1),
                        random_generator=np.random.RandomState(noise_seed)
                       )
            S=add_jitter(S,
                         jitter_max_timedelata=pd.to_timedelta(gen_pattern['target_interval_seconds']*gen_pattern['jitter'],unit='s'),
                         random_generator=np.random.RandomState(noise_seed)
                        )
            S=add_offset(S,
                         offset_timedelta=pd.to_timedelta(
                             gen_pattern.get('target_interval_seconds') * \
                                 gen_pattern.get('spread_offset', 1.0) * dup_id / gen_pattern['duplication_count'],
                             unit='s')
                         )
            subdf=series_to_df(S,tags)
            #if type(finaldf) is type(None):
            #    finaldf=subdf.copy()
            #else:
            #    finaldf=finaldf.append(subdf)#,sort=True)
            finaldfs.append(subdf)
    if len(finaldfs):
        finaldf=pd.concat(finaldfs).dropna(subset=['VALUE']).fillna('-')
        del finaldfs
    else:
        finaldf=pd.DataFrame()
    khatam=datetime.utcnow()
    return finaldf,khatam-shuru


def generate_all_clean_series():
    """
    Generate a single example for each configured pattern at the target frequency

    Returns
    -------
    None :
        The examples will be added to the internal configuration object.
    """
    shuru,total_points,total_size=datetime.utcnow(),0,0
    print("Started one-time generate_all_clean_series at {}".format(shuru))

    base_patterns = CONF['base_patterns']
    for gen_pattern in CONF['generated_patterns']:
        if gen_pattern.get("disabled", False):
            continue
        model = gen_pattern['model']
        base_pattern_name = gen_pattern['base_pattern']
        interval_secs = gen_pattern['target_interval_seconds']
        generated_series = generate_clean_series_from_base_pattern(
            base_pattern_values=base_patterns[base_pattern_name],
            target_interval_secs=interval_secs,
            model=model
        )
        scaled_generated_series = generated_series * gen_pattern['scaling']
        total_points+=len(scaled_generated_series)
        total_size+=sys.getsizeof(scaled_generated_series)
        gen_pattern['clean_series'] = scaled_generated_series
    # end
    khatam=datetime.utcnow()
    print("Ending one-time generate_all_clean_series at {}.".format(khatam))
    print("It took {} time to generate {} data-points blocking {} conf-size".format(khatam-shuru, total_points, sizeof_fmt(total_size)))
    # estimate run-time memory consumption:
    temp_df,gtime=generate_all_series_for_one_day(0,pd.Timedelta(0),pd.Timedelta(CONF['jobs']['generate_window_min'],unit='m'),0)
    runsize,runlen=sys.getsizeof(temp_df),len(temp_df)
    del temp_df
    print("Dry Run: {} runtime-size ({} points across {} minutes) consuming {} generation time.".format(sizeof_fmt(runsize), runlen, CONF['jobs']['generate_window_min'],gtime))
    return gtime


def init_writer(destdb, measurement_name,isbatch=False):
    from threading import Thread, Event
    from queue import Queue
    class Obj:pass
    self=Obj()
    self.evt = Event()
    self.write_queue = Queue()
    self.write_thread = Thread(target=_write_points_from_queue,
                               args=(self,self.write_queue,),
                               name='write-to-target-influxdb-Thread',
                               daemon=True)
    self.evt.set()  # indicate 'ready'
    self.measurement_name=measurement_name
    targetdb_host,targetdb_port,targetdb_user,targetdb_pass,targetdb_dbname=destdb.split(":")
    if targetdb_host == 'localhost':
        targetdb_host = '127.0.0.1'
    # If the port number starts with '*' then make it SSL
    if targetdb_port[0] == '*':
        ssl_enabled = True
        targetdb_port = targetdb_port[1:]
    else:
        ssl_enabled = False

    if isbatch:
        self.targetdb = influxdb.DataFrameClient(
            host=targetdb_host,
            port=targetdb_port,
            username=targetdb_user,
            password=targetdb_pass,
            database=targetdb_dbname,
            ssl=ssl_enabled,
            verify_ssl=False
        )
    else:
        client = influxdb.InfluxDBClient(host=targetdb_host, port=targetdb_port, username=targetdb_user, password=targetdb_pass,
                                        ssl=ssl_enabled, verify_ssl=False)
        client.switch_database(targetdb_dbname)
        self.targetdb=client

    # TODO: ping the databases
    # create target db, in case it doesn't yet exist
    #self.targetdb.create_database(targetdb_dbname)
    self.prevt3=None
    self.total_points=0
    self.write_thread.start()
    return self

# --- write to influx funcs : written in levels to do queues - twice ( once for batching large pushes, other for the whole push )
def _write_points_from_queue(self, in_q):
    while True:
        (df_to_write, m_name, tag_columns, isbatch) = in_q.get()
        # Break up dataframe if it is too large to write on one go
        df_len = df_to_write.shape[0]
        print("WRITER QUEUE- Task to write {} points of range [ {} - {}]".format(df_len, df_to_write.index.min(),df_to_write.index.max()))
        i0 = 0
        def toline(old,m_name=m_name):
            new={}
            new['measurement']=m_name
            new['tags']={k:v for k,v in old.items() if k.count('level') or k.count('metric') or k.count('monitor') or k.count('host')}
            new['fields']={"VALUE":old["VALUE"]}
            return new
        while i0 < df_len:
            i1 = i0 + MAX_WRITE_BATCH_SIZE
            if i1 > df_len:
                i1 = df_len
            df_chunk = df_to_write.iloc[i0:i1]
            # write the df_chunk
            while True:
                try:
                    if isbatch:
                        self.targetdb.write_points(df_chunk, m_name, tag_columns=tag_columns)
                    else:
                        self.targetdb.write_points(list(map(toline,json.loads(df_chunk.reset_index(drop=True).to_json(orient='records')))))
                    break
                except influxdb.exceptions.InfluxDBClientError as e:
                    print(e)
                    print("DF Tags = " + str(tag_columns))
                    print("DF Columns = " + str(df_to_write.columns))
                    print("Skipping this write")
                    break
                except Exception as e:
                    print(e)

            i0 = i1

        # set the event flag to permit the next write request to be sent into the queue
        self.evt.set()


def _write(self, df,isbatch=False): #, start_ts=None, end_ts=None):
    # Send the dataframe for writing to the target database
    t2 = datetime.utcnow()
    self.evt.wait()      # wait for the *previous* write event to complete
    self.evt.clear()     # indicate the write thread is now busy
    t23 = datetime.utcnow()
    prev_write_time = (t23-self.prevt3).total_seconds() if self.prevt3 else 0
    logline="Prev_Write[+Next_Generate] completed in {:.3f} secs.".format(prev_write_time)
    print(logline)
    self.write_queue.put((df.copy(), self.measurement_name, sorted(set( df.columns)-set(["VALUE"])),isbatch ))   # This will set 'evt' when finished
    t3 = datetime.utcnow()
    write_wait_time = (t3 - t2).total_seconds()
    logline="Next write initiated after waiting for {:.3f} secs. \nStarting write of {} data-points of [ {} - {} ]."
    logline=logline.format(write_wait_time, len(df), df.index.min(), df.index.max())
    print(logline)
    self.prevt3=t3
    self.total_points+=len(df)
    return prev_write_time,write_wait_time

def _writer_end(self):
    t2 = datetime.utcnow()
    self.evt.wait()      # wait for the *previous* write event to complete
    t23 = datetime.utcnow()
    prev_write_time = (t23-self.prevt3).total_seconds() if self.prevt3 else 0
    write_wait_time = (t23 - t2).total_seconds()
    logline="Write completed in {:.3f} secs. Waited for {:.3f} secs to complete it.".format(prev_write_time,write_wait_time)
    print(logline)

def realtime_sleep(curend):
    """ sleep until now becomes greater than curend """
    if CONF['jobs']["mode"]=="realtime":
        wait_for=(datetime(*list(datetime.utcnow().utctimetuple())[:7],tzinfo=timezone.utc)-curend).total_seconds()
        if wait_for<0:
            print("Sleeping for {} secs to just let stuff get real.".format(-wait_for))
            time.sleep(wait_for*-0.999999999)

def write(writer, df,window=5):
    "write the df to db iterating slices by window seconds"
    window=timedelta(seconds=window)
    #break df by window seconds:
    start=df.index.min() # df.index[0]
    end=df.index.max() # df.index[-1]
    cur, curend = start, start+window
    write_time, write_wait_time=0,0
    while cur<=end:
        # chunking of df by window time
        #subdf=df[cur:curend]
        subdf=df[(df.index>=cur)&(df.index<curend)]
        if len(subdf):
            realtime_sleep(subdf.index.max())  # dont copy to future, instead wait for it to become now.
            wt,wwt=_write(writer, subdf,isbatch=CONF['jobs']["mode"]=="batch")#, start_ts=cur, end_ts=curend)
            write_time, write_wait_time = write_time+wt, write_wait_time+wwt
        cur,curend=curend,curend+window
    return write_time, write_wait_time
def Write(writer,df,window=5,thread=[]):
    from threading import Thread
    newT=Thread(target=write,args=[writer,df,window],name="writer-thread")
    if not len(thread):
        #create new thread
        thread.append(newT)
    if thread[0].is_alive():
        print("JOIN:Waiting for previous Write Job to finish.")
        thread[0].join()
        print("JOIN:Completed wait for previous Write Job to finish.")
    if not thread[0].is_alive():
        #kill and create a new thread
        thread[0]=newT
    # put task to this thread
    newT.start()
    #move on
    return


def launch_jobs(delay=pd.Timedelta(0)):
    shuru=datetime.utcnow()
    print("Started the great synthetic generator at {}".format(shuru))
    # start
    destdb=CONF['jobs']['destdb']
    writewindow=CONF['jobs']["writewindow_sec"]
    generate_window=CONF['jobs']["generate_window_min"]
    dbwriter=init_writer(destdb=destdb, measurement_name=CONF['jobs']['measurement'],isbatch=CONF['jobs']["mode"]=="batch")
    if CONF['jobs']["mode"]=="batch":
        start_date,end_date=datetime_parser(CONF['jobs']['start-datetime']),datetime_parser(CONF['jobs']['end-datetime'])

    elif CONF['jobs']["mode"]=="realtime":
        now=datetime(*list((datetime.utcnow()+delay*1.5).utctimetuple())[:7],tzinfo=timezone.utc)
        start_date, end_date = now, now+timedelta(365)

    elif CONF['jobs']["mode"]=="auto":
        # TODO: db queries to fill gaps in last 30 days, then start realtime mode
        raise NotImplementedError
    else:
        raise ValueError("Invalid jobs.mode: {}".format(CONF['jobs']['mode']))

    #iterate daywise thorugh-out the range(start_date,end_date)

    for cur_start,cur_end in rangetime(start_date,end_date):
        print("Working on DAY: {} - {}".format(cur_start,cur_end))
        for istart,iend,I in rangetimedelta_custom(cur_start, cur_end, step=pd.Timedelta(generate_window,unit='m')):
            df_to_push, gtime = generate_all_series_for_one_day(int(cur_start.day/7), istart, iend, I)
            if not len(df_to_push):continue
            df_to_push.index = (df_to_push.index + to_midnight(cur_start)).tz_convert(timezone.utc)
            print("Generation took {} time for creating df of [ {} - {} ]".format(gtime,\
                                                                                          df_to_push.index.min(), df_to_push.index.max()))
            df_to_push=df_to_push[(df_to_push.index>=cur_start) & (df_to_push.index<cur_end)]
            if not len(df_to_push):continue
            Write(dbwriter,df_to_push, window=writewindow)
    _writer_end(dbwriter)  # wait for previous writes to complete before terminating
    # end
    khatam=datetime.utcnow()
    print("Ending the great synthetic generator at {}.\n It took {} time to write {} data-points completely.".format(khatam, khatam-shuru, dbwriter.total_points))

if __name__ == '__main__':
    self=sys.argv.pop(0)
    if len(sys.argv) < 1:
        usage="\nUsage:\n\t{} {} /path/to/conf/file".format(sys.executable,self)
        print("Please provide config filename as command line argument."+usage, file=sys.stderr)
        exit(1)
    conf_filename = sys.argv[0]
    load_config(conf_filename)
    if "+history" in sys.argv:
        forked_mode = 'realtime' if os.fork() else 'batch'
        load_config(conf_filename, forked_mode)
        CONF['jobs']['mode'] = forked_mode
        CONF['jobs']['start-datetime'] = (datetime.utcnow()-pd.Timedelta(days=3*30)).strftime("%Y%m%d%H%M")
        CONF['jobs']['end-datetime'] = datetime.utcnow().strftime("%Y%m%d%H%M")
    #place_stop(name=CONF['jobs'].get("stop_name","")+forked_mode)
    """
    Prepare each time series at the start of each day.
    Each day must have different noise, otherwise won't be able to train from history
    Can use same anomalies each day. Add noise after.
    Jitter must exist BETWEEN series, to spread out the duplicates in the time domain.
    Not sure if jitter needs to change each day for each point in a single given series.
    Adding jitter for the timestamp interval risks going past 24:00 after the duplicate time offset was added.
    """
    delay=generate_all_clean_series()
    # now we have all clean ones in our CONF.generated_patterns[i].clean_series
    # now for each day we need to generate data, call generate_all_series_for_one_day
    # and start pushing that to db
    launch_jobs(delay)


#run as : python -m cProfile -s time syn_gen.py syn_gen.conf > timed.html

