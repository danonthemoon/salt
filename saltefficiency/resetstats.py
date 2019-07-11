import sys
import time
import datetime
import string
import numpy as np
import mysql

from blockvisitstats import getnightinfo

def resetstats(sdb, obsdate):
   #for a given obsdate get the night info
   nid=getnightinfo(sdb, obsdate)
   print(nid)
   selcmd='BlockVisit_Id, BlockVisitStatus_Id, Proposal_Code, Block_Id'
   tabcmd='Block join BlockVisit using (Block_Id) join Proposal using (Proposal_Id) join ProposalCode on (Proposal.ProposalCode_Id = ProposalCode.ProposalCode_Id)'
   blocks=sdb.select(selcmd, tabcmd, 'NightInfo_Id=%i' % nid)
   blocks = list(blocks)
   print(blocks)
   bvid_list=[]
   for b in blocks:
      print('22')
      bvid_list.append(b[0])
   print(bvid_list)
   for bvid in bvid_list:
      inscmd='SlewTime=0, TrackerSlewTime=0, TargetAcquisitionTime=0'
      sdb.update(inscmd, 'BlockVisit', 'BlockVisit_Id=%i' % bvid)
      inscmd='InstrumentAcquisitionTime=0, ScienceTrackTime=0'
      sdb.update(inscmd, 'BlockVisit', 'BlockVisit_Id=%i' % bvid)
   return bvid_list
