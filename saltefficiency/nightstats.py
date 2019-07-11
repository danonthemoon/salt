"""
Created July 2019

@author Danny Sallurday

For a given observation date, calculate and return the median values of overhead stats for each instrument.

These stats are:

Slew Time (Point Command - Track Start)
Tracker Slew Time (Track Start - On Target)
Target Acquisition Time (On Target - First Salticam Image)
Instrument Acquisition Time (First Acquisition Image - First Science Image)
Science Track Time (First Science Image - Track End)

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

   # iterate through blockvisits to accumulate & obtain median over the number of accepted blockvisits
   rss_slew=[]
   rss_trslew=[]
   rss_targetacq=[]
   rss_instracq=[]
   rss_scitrack=[]
   hrs_slew=[]
   hrs_trslew=[]
   hrs_targetacq=[]
   hrs_instracq=[]
   hrs_scitrack=[]
   count=0
   for bvid in blockvisits:
       selcmd='SlewTime, TrackerSlewTime, TargetAcquisitionTime, InstrumentAcquisitionTime, ScienceTrackTime'
       tabcmd='BlockVisit'
       scistats=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
       if not all(scistats[0]):
           continue
       selcmd='INSTRUME'
       tabcmd='FileData'
       bv_inst=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
       if len(bv_ins)==0: continue
       if bv_inst[0][0]=='RSS':
          rss_slew.append(scistats[0][0])
          rss_trslew.append(scistats[0][1])
          rss_targetacq.append(scistats[0][2])
          rss_instracq.append(scistats[0][3])
          rss_scitrack.append(scistats[0][4])
      elif bv_inst[0][0]=='HRS':
          hrs_slew.append(scistats[0][0])
          hrs_trslew.append(scistats[0][1])
          hrs_targetacq.append(scistats[0][2])
          hrs_instracq.append(scistats[0][3])
          hrs_scitrack.append(scistats[0][4])
       else: continue
       count+=1
   if count == 0:
       nightstats = []
   else:
       nightstats = [rss_slew,rss_trslew,rss_targetacq,rss_instracq,rss_scitrack,hrs_slew,hrs_trslew,hrs_targetacq,hrs_instracq,hrs_scitrack]
   return nightstats, count
