import os
import sys, time, datetime, string
import struct
import numpy as np
import sdb_mysql as mysql

from blockvisitstats import getnightinfo

if __name__=='__main__':
   """Determine the overhead statistics for a specific observation date.

       Parameters
       ----------
       obsdate: string
            observation date of interest

   """
   sdb=mysql.mysql('sdbsandbox.cape.saao.ac.za', 'sdb_v7', 'danny', 'lemmein!', port=3306)

   sdate = sys.argv[1]
   edate=sys.argv[2]
   sdate = datetime.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
   edate = datetime.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
   date = sdate
   while date <= edate:
       obsdate = '%4i-%2s-%2s' % (date.year, str(date.month).zfill(2), str(date.day).zfill(2))
       print (obsdate)
       results=resetstats(sdb, obsdate)
       date += datetime.timedelta(days=1)



def resetstats(sdb, obsdate):
   #for a given obsdate get the night info
   nid=getnightinfo(sdb, obsdate)

   selcmd='BlockVisit_Id, BlockVisitStatus_Id, Proposal_Code, Block_Id'
   tabcmd='Block join BlockVisit using (Block_Id) join Proposal using (Proposal_Id) join ProposalCode on (Proposal.ProposalCode_Id = ProposalCode.ProposalCode_Id)'
   blocks=sdb.select(selcmd, tabcmd, 'NightInfo_Id=%i' % nid)
   bvid_list=[]
   for b in blocks:
       if b[1]==1:
          bvid_list.append(b[0])
   for bvid in bvid_list:
      inscmd='SlewTime=0, TrackerSlewTime=0, TargetAcquisitionTime=0'
      sdb.update(inscmd, 'BlockVisit', 'BlockVisit_Id=%i' % bvid)
      inscmd='InstrumentAcquisitionTime=0, ScienceTrackTime=0'
      sdb.update(inscmd, 'BlockVisit', 'BlockVisit_Id=%i' % bvid)
   return bvid_list
