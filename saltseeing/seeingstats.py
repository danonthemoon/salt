"""
Created August 2019

@author Danny Sallurday

For a given observation date, track inside and outside seeing stats for each block.

"""
import os
import sys, time, datetime, string
import struct
import numpy as np
import sdb_mysql as mysql

def getnightinfo(sdb, obsdate):
    return sdb.select('NightInfo_Id', 'NightInfo', 'Date=\'%s\'' % obsdate)[0][0]

def getseeingstats(sdb, obsdate):
   """Determine the overhead statistics for a specific observation date.

       Parameters
       ----------
       sdb: ~mysql.mysql
          A connection to the sdb database
       obsdate: str
          Observing date in YYYYMMDD format
   """

   #get the NightInfo_Id for the given obsdate
   nid=getnightinfo(sdb, obsdate)

   #get the times for the night
   record=sdb.select('EveningTwilightEnd, MorningTwilightStart', 'NightInfo', 'NightInfo_Id=%i' % nid)
   stime=record[0][0]
   etime=record[0][1]
   totaltime=(etime-stime).seconds

   #From the sdb, get the SoLogEvent table
   record=sdb.select('EventType_Id, EventTime', 'SoLogEvent', 'NightInfo_Id=%i' % nid)
   event_list=[]
   for i in range(len(record)):
       r=record[i]
       if r[1].seconds>43200:
          t=datetime.datetime(int(obsdate[0:4]), int(obsdate[5:7]), int(obsdate[8:10]), 0, 0, 0)+r[1]
       else:
          t=datetime.datetime(int(obsdate[0:4]), int(obsdate[5:7]), int(obsdate[8:10]), 0, 0, 0)+datetime.timedelta(days=1)+r[1]
       event_list.append([r[0], t])
   #sort the list by the datetimes
   event_list.sort(key=lambda e:e[1])

   #get the night's list of blocks
   selcmd='BlockVisit_Id'
   tabcmd='BlockVisit'
   logcmd='NightInfo_Id=%i and BlockVisitStatus_Id=%i' % (nid, 1)
   bvid_list=sdb.select(selcmd, tabcmd, logcmd)
   bvid_list[:]=[bvid[0] for bvid in bvid_list]

   #get a list of all point commands from the night
   select_state= 'BlockVisit_Id, EventTime, Block_Id, Target_Name, NightInfo_Id, EventData'
   table_state='PointEvent join SoLogEvent using (SoLogEvent_Id)'
   plist=sdb.select(select_state, table_state, 'NightInfo_Id=%i' % nid)
   point_list=[]
   for i in range(len(plist)):
       p=plist[i]
       if p[1].seconds>43200:
          t=datetime.datetime(int(obsdate[0:4]), int(obsdate[5:7]), int(obsdate[8:10]), 0, 0, 0)+p[1]
       else:
          t=datetime.datetime(int(obsdate[0:4]), int(obsdate[5:7]), int(obsdate[8:10]), 0, 0, 0)+datetime.timedelta(days=1)+p[1]
       point_list.append([p[0], t, p[2], p[3], p[4], p[5]])

   nightdeltas=[]
   #deal with accepted blocks
   for bvid in bvid_list:

      #determine start time (point) and end time (track end)
       pointtime = findpointcommand(bvid, point_list)
       if pointtime is None:
           print('no point')
           continue
       starttime=pointtime
       endtime=findguidingstop(starttime, event_list)
       if endtime is None:
           print('no end')
           continue

       #determine total time
       tottime=endtime-starttime
       #some limit to avoid crazy stats
       if tottime.seconds > 10000:
           print('total too long, took %i s'%tottime.seconds)
           continue


       seeingstart=0
       seeingend=0
       delta = seeingend-seeingstart
       nightdeltas.append(seeingdelta)
