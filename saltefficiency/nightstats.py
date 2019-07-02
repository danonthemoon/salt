"""
Created July 2019

@author Danny Sallurday

For a given observation date, calculate and return the mean values for:

0: Slew Time (Point Command - Track Start)
1: Tracker Slew Time (Track Start - On Target)
2: Target Acquisition Time (On Target - First Salticam Image)
3: Instrument Acquisition Time (First Salticam Image - First Science Image)
4: Science Track Time (First Science Image - Track End)

"""
import os
import sys, time, datetime, string
import struct
import numpy as np
import sdb_mysql as mysql

from blockvisitstats import getnightinfo

def getnightstats(sdb, obsdate):
   """Determine the overhead statistics for a specific observation date.

       Parameters
       ----------
       obsdate: string
            observation date of interest

   """

   #for a given obsdate get the night info
   nid=getnightinfo(sdb, obsdate)

   #get the list of block visits on obsdate
   selcmd='BlockVisit_Id'
   tabcmd='BlockVisit'
   blockvisits=sdb.select(selcmd, tabcmd, 'NightInfo_Id=%i' % nid)
   blockvisits=list(blockvisits)

   # iterate through blockvisits to accumulate & average over the number of accepted blockvisits
   nightslew=0
   nighttrslew=0
   nighttargetacq=0
   nightinstracq=0
   nightscitrack=0
   count=0
   for bvid in blockvisits:
       selcmd='SlewTime, TrackerSlewTime, TargetAcquisitionTime, InstrumentAcquisitionTime, ScienceTrackTime'
       tabcmd='BlockVisit'
       bvstats=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
       if not all(bvstats[0]):
           continue
       else:
          count+=1
          nightslew+=bvstats[0][0]
          nighttrslew+=bvstats[0][1]
          nighttargetacq+=bvstats[0][2]
          nightinstracq+=bvstats[0][3]
          nightscitrack+=bvstats[0][4]

   nightstats = [nightslew/count,nighttrslew/count,nighttargetacq/count,nightinstracq/count,nightscitrack/count]
   return nightstats
