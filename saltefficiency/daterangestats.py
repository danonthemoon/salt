"""
Created July 2019

@author Danny Sallurday

Script to generate plots for overhead stats averaged over a given range of dates.

"""

import os
import sys, time, datetime, string
import struct
import numpy as np
import sdb_mysql as mysql
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
plt.switch_backend('agg')

import run_blockvisitstats
from nightstats import getnightstats

if __name__=='__main__':
   elshost='db2.suth.saao.ac.za'
   elsname='els'
  # elsuser=os.environ['ELSUSER']
  # elspassword=os.environ['SDBPASS']
   sdbhost='sdb.salt'
   sdbname='sdb'
  # user=os.environ['SDBUSER']
  # password=os.environ['SDBPASS']

   sdb=mysql.mysql('sdbsandbox.cape.saao.ac.za', 'sdb_v7', 'danny', 'lemmein!', port=3306)

   sdate = sys.argv[1]
   edate = sys.argv[2]
   startdate = datetime.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
   enddate = datetime.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
   date = startdate
   slewtotal=0
   trslewtotal=0
   targetacqtotal=0
   instracqtotal=0
   scitracktotal=0
   count=0
   while date <= enddate:
       obsdate = '%4i-%2s-%2s' % (date.year, str(date.month).zfill(2), str(date.day).zfill(2))
       nightstats=getnightstats(sdb, obsdate)
       date += datetime.timedelta(days=1)
       if len(nightstats) == 0: continue
       else:
          count+=1
          slewtotal+=nightstats[0]
          trslewtotal+=nightstats[1]
          targetacqtotal+=nightstats[2]
          instracqtotal+=nightstats[3]
          scitracktotal+=nightstats[4]
   if count == 0:
       print("No observation nights within this range")
       rangestats=[0]
   else:
       rangestats = {}
       rangestats.update({'SlewTime' : slewtotal/count, 'TrackerSlewTime' : trslewtotal/count})
       rangestats.update({'TargetAcquisitionTime':targetacqtotal/count, 'InstrumentAcquisitionTime':instracqtotal/count})
       rangestats.update({'ScienceTrackTime': scitracktotal/count})

   #Produce a pdf with the relevant stats
   with PdfPages('overheadstats-%s-%s.pdf' % (sdate, edate)) as pdf:
       fig = plt.figure(figsize=(8.27, 11.69))
       df = pd.DataFrame(rangestats)
       df.plot(kind="bar", stacked=True)
       #plt.bar(range(len(rangestats)), list(rangestats.values()), align='center')
       plt.ylabel("Time (s)")
       plt.title('Overhead Statistics for %s to %s' % (sdate,edate))
       #plt.yticks(np.arange(0, 300, 30))
       #plt.xticks(range(len(rangestats)), list(rangestats.keys()))
       pdf.savefig(fig)
       plt.show()  # saves the current figure into a pdf page
       plt.close()
