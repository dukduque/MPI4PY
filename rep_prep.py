import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import time
from collections import defaultdict
import collections


#-----

df = pd.read_csv('/artspace/janna/reports/b1011/b1011_10-16_03-18.csv', sep="|") 
df=df.rename(columns={'User': 'netID', "Class": "Queue"})
#for testing

#df=df[46888:46890]
df['StartTime'] = pd.to_datetime(df['StartTime'])
df['SubmitTime'] = pd.to_datetime(df['SubmitTime'])
df['EndTime'] = pd.to_datetime(df['EndTime'])
df.sort_values(['StartTime', 'EndTime'], axis=0, ascending=True, inplace=True, kind='quicksort', na_position='last')

#-----

#function for dealing with dates
def date_to_secs(date_string):
    s = date_string
    #d = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return(time.mktime(s.timetuple()))

#-----

#What was the average load, in number of processors active?
#Note that as written this takes samples at certain intervals.  The finest this data
#goes is to the minute
#this takes a while - could improve the algo - order the data by time then do a binary search

submit_secs=[]
start_secs=[]
end_secs=[]
procs=[]
secs=[]

for entry in df.StartTime:
    start_secs.append(int(date_to_secs(entry)))
  
for entry in df.SubmitTime:
    submit_secs.append(int(date_to_secs(entry)))
    
for entry in df.EndTime:
    end_secs.append(int(date_to_secs(entry)))

for entry in df.Processors:
    procs.append(entry)
    
secs = list(zip(submit_secs, start_secs, end_secs, procs))

first_job = date_to_secs(df.SubmitTime.min())
last_job = date_to_secs(df.SubmitTime.max())
REPORT_first_date = df.SubmitTime.min()
REPORT_last_date = df.SubmitTime.max()

print(first_job, last_job)

#set up the time array
time_instance = first_job
times=[]
times.append(first_job)

sample_interval_min=1
sample_interval_sec = sample_interval_min * 60

while time_instance < last_job:
    time_instance=time_instance + sample_interval_sec 
    times.append(time_instance)

time_length=(len(times))

#dates for the labels on the graph
label_secs=[]
label_dates=[]
number_of_labels=18#  <<<<---------------CHANGE NUMBER OF LABELS HERE------------------->>>
distance_between_dates=int((last_job-first_job)/number_of_labels)
date_label=0
label_seconds = first_job
for i in range(number_of_labels):
    label_secs.append(label_seconds)
    label_seconds = label_seconds + distance_between_dates
    
for date_in_secs in label_secs:
    label_dates.append(time.strftime('%Y-%m-%d', time.localtime(date_in_secs)))

print(label_dates)

usage_dict={}

time_count=0
limit_outer=0
limit_inner=0

for time in times:
    usage_dict[time]=0
    for second in secs:
        if (time >= second[1]) and (time < second[2]):
            usage_dict[time]=usage_dict[time]+second[3] 
        elif (time < second[1]):
            break
    time_count=time_count+1
print('yup')

file = open('/artspace/janna/reports/b1011/b1011_usage_dict.csv', 'w')
for key, value in usage_dict.items():
    stringy = (str(int(key)) + ',' + (str(value)))
    file.write(str(stringy))
    file.write('\n')
file.close() 
f.close()
