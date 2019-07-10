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
   nightslew=[]
   nighttrslew=[]
   rss_targetacq=[]
   rss_instracq=[]
   rss_scitrack=[]
   hrs_targetacq=[]
   hrs_instracq=[]
   hrs_scitrack=[]
   count=0
   for bvid in blockvisits:
       selcmd='SlewTime, TrackerSlewTime'
       tabcmd='BlockVisit'
       slewstats=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
       if not all(slewstats[0]):
           continue
       nightslew.append(slewstats[0][0])
       nighttrslew.append(slewstats[0][1])
       selcmd='TargetAcquisitionTime, InstrumentAcquisitionTime, ScienceTrackTime'
       tabcmd='BlockVisit'
       scistats=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
       if not all(scistats[0]):
           continue
       selcmd='INSTRUME'
       tabcmd='FileData'
       bv_ins=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
       if len(bv_ins)==0: continue
       if bv_ins[0][0]=='RSS':
          rss_targetacq.append(scistats[0][0])
          rss_instracq.append(scistats[0][1])
          rss_scitrack.append(scistats[0][2])
       elif bv_ins[0][0]=='HRS':
          hrs_targetacq.append(scistats[0][0])
          hrs_instracq.append(scistats[0][1])
          hrs_scitrack.append(scistats[0][2])
       count+=1
   if count == 0:
       nightstats = []
   else:
       nightstats = [nightslew,nighttrslew,rss_targetacq,rss_instracq,rss_scitrack,hrs_targetacq,hrs_instracq,hrs_scitrack]
   return nightstats, count
