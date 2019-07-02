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
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

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
   sdate = datetime.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
   edate = datetime.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
   date = sdate
   slewtotal=0
   trslewtotal=0
   targetacqtotal=0
   instracqtotal=0
   scitracktotal=0
   count=0
   while date <= edate:
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

   rangestats = [slewtotal/count,trslewtotal/count,targetacqtotal/count,instracqtotal/count,scitracktotal/count]

   # Create the PdfPages object to which we will save the pages:
   # The with statement makes sure that the PdfPages object is closed properly at
   # the end of the block, even if an Exception occurs.
   with PdfPages('multipage_pdf.pdf') as pdf:
       plt.figure(figsize=(3, 3))
       plt.plot(rangestats)
       plt.title('Page One')
       pdf.savefig()  # saves the current figure into a pdf page
       plt.close()
