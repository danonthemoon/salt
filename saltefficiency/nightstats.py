"""
Created July 2019

@author Danny Sallurday

For a given observation date, accumulate overhead stats from block visits
from the database and return the median values.

These stats are:

Slew Time (Point Command - Track Start)
Tracker Slew Time (Track Start - On Target)
Target Acquisition Time (On Target - First Salticam Image)
Instrument Acquisition Time (First Acquisition Image - First Science Image)

They are also seperated by the primary instrument of each block. (RSS/HRS)

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
       sdb: mySQL database
            database from which to obtain overhead stats
       obsdate: string
            observation date of interest
   """

   #get the NightInfo_Id for the given obsdate
   nid=getnightinfo(sdb, obsdate)

   #get the list of all block visits on obsdate
   selcmd='BlockVisit_Id, BlockVisitStatus_Id'
   tabcmd='BlockVisit'
   blockvisits=sdb.select(selcmd, tabcmd, 'NightInfo_Id=%i' % nid)
   blockvisits=list(blockvisits)

   #get list of only accepted blocks
   bvid_list=[]
   for b in blockvisits:
       if b[1]==1:
          bvid_list.append(b[0])

   #iterate through blockvisits to accumulate & obtain median over the accepted blockvisits
   rss_slew=[]
   rss_trslew=[]
   rss_targetacq=[]
   rss_instracq=[]
   hrs_slew=[]
   hrs_trslew=[]
   hrs_targetacq=[]
   hrs_instracq=[]
   mos_slew=[]
   mos_trslew=[]
   mos_acq=[]
   instrument=''
   rss_count=0
   hrs_count=0
   mos_count=0
   bid=''
   for bvid in bvid_list:
       selcmd='INSTRUME'
       tabcmd='FileData'
       bv_instruments=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
       for i in bv_instruments:
          if 'RSS' in i:
              select_state= 'Block_Id'
              table_state='BlockVisit'
              logic_state='BlockVisit_Id=%i' % (bvid)
              bids=sdb.select(select_state, table_state, logic_state)
              if not bids:
                  continue
              else:
                  bid=bids[0]
              selcmd='Block_Id, Barcode'
              tabcmd='Block join Pointing using (Block_Id) join Observation using (Pointing_Id) '
              tabcmd+='join TelescopeConfigObsConfig using (Pointing_Id) join ObsConfig on (PlannedObsConfig_Id=ObsConfig_Id) '
              tabcmd+='join RssPatternDetail using (RssPattern_Id) join Rss using (Rss_Id) join RssProcedure using (RssProcedure_Id) '
              tabcmd+='join RssConfig using (RssConfig_Id) join RssMask using (RssMask_Id)'
              logcmd='RssProcedureType_Id = \'7\' and Block_Id = %i group by Block_Id order by Block_Id' % bid
              mos=sdb.select(selcmd, tabcmd, logcmd)
              if mos:
                  instrument='MOS'
              else: instrument='RSS'
              break
          elif 'HRS' in i:
              instrument='HRS'
              break

       if instrument=='MOS':
           selcmd='SlewTime, TrackerSlewTime, MOSAcquisitionTime'
           tabcmd='BlockVisit'
           mosscistats=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
           if not all(mosscistats[0]): continue
       else:
          selcmd='SlewTime, TrackerSlewTime, TargetAcquisitionTime, InstrumentAcquisitionTime'
          tabcmd='BlockVisit'
          scistats=sdb.select(selcmd, tabcmd, 'BlockVisit_Id=%i' % bvid)
          if not all(scistats[0]): continue

       if instrument=='RSS':
          rss_slew.append(scistats[0][0])
          rss_trslew.append(scistats[0][1])
          rss_targetacq.append(scistats[0][2])
          rss_instracq.append(scistats[0][3])
          rss_count+=1
       elif instrument=='HRS':
          hrs_slew.append(scistats[0][0])
          hrs_trslew.append(scistats[0][1])
          hrs_targetacq.append(scistats[0][2])
          hrs_instracq.append(scistats[0][3])
          hrs_count+=1
       elif instrument=='MOS':
          mos_slew.append(mosscistats[0][0])
          mos_trslew.append(mosscistats[0][1])
          mos_acq.append(mosscistats[0][2])
          mos_count+=1

   if rss_count==0 and hrs_count==0 and mos_count==0:
       nightstats = []
   else:
       nightstats = [rss_slew,rss_trslew,rss_targetacq,rss_instracq,hrs_slew,hrs_trslew,hrs_targetacq,hrs_instracq,mos_slew,mos_trslew,mos_acq]
   return nightstats, rss_count, hrs_count, mos_count
