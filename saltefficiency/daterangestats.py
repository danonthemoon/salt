"""
Created July 2019

@author Danny Sallurday

Script to generate a pdf plot of overhead stats over a given range of dates.

"""

import os
import sys, time, datetime, string
import struct
import numpy as np
import sdb_mysql as mysql
import pandas as pd
import matplotlib.pyplot as plt
from statistics import median
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

   #accumulate all of the overhead stats over the given range of dates
   rss_slewtimes=[]
   rss_trslewtimes=[]
   rss_targetacqtimes=[]
   rss_instracqtimes=[]
   rss_scitracktimes=[]
   hrs_slewtimes=[]
   hrs_trslewtimes=[]
   hrs_targetacqtimes=[]
   hrs_instracqtimes=[]
   hrs_scitracktimes=[]
   nights=0
   blocks=0
   while date <= enddate:
       obsdate = '%4i-%2s-%2s' % (date.year, str(date.month).zfill(2), str(date.day).zfill(2))
       nightstats, numberofblocks = getnightstats(sdb, obsdate)
       date += datetime.timedelta(days=1)
       if len(nightstats) == 0 or numberofblocks == 0: continue
       else:
          rss_slewtimes.extend(nightstats[0])
          rss_trslewtimes.extend(nightstats[1])
          rss_targetacqtimes.extend(nightstats[2])
          rss_instracqtimes.extend(nightstats[3])
          hrs_slewtimes.extend(nightstats[4])
          hrs_trslewtimes.extend(nightstats[5])
          hrs_targetacqtimes.extend(nightstats[6])
          hrs_instracqtimes.extend(nightstats[7])
       blocks+=numberofblocks
       nights+=1
   if nights == 0:
       print("No valid observation nights within this range")
   else:
       rss_stats = {}
       rss_stats.update({'1. Slew' : median(rss_slewtimes)})
       rss_stats.update({'2. Tracker Slew' : median(rss_trslewtimes)})
       rss_stats.update({'3. Target Acquisition': median(rss_targetacqtimes)})
       rss_stats.update({'4. Instrument Acquisition': median(rss_instracqtimes)})
       hrs_stats = {}
       hrs_stats.update({'1. Slew' : median(hrs_slewtimes)})
       hrs_stats.update({'2. Tracker Slew' : median(hrs_trslewtimes)})
       hrs_stats.update({'3. Target Acquisition': median(hrs_targetacqtimes)})
       hrs_stats.update({'4. Instrument Acquisition': median(hrs_instracqtimes)})

   #produce a pdf with the relevant stats, distinguished by instrument
   with PdfPages('blockoverheadstats-%s-%s.pdf' % (sdate, edate)) as pdf:
       #plot RSS and HRS stats as different bars
       stats = [rss_stats, hrs_stats]
       df = pd.concat([pd.Series(d) for d in stats], axis=1).fillna(0).T
       df.index = ['RSS Stats', 'HRS Stats']
       ax = df.plot(kind="bar", stacked=True, colormap='Paired', figsize=(8.27,11.69))

       #label the bar splits and totals
       heights = []
       for patch in ax.patches:
           heights.insert(0, patch.get_height())
       rss_heights=[]
       hrs_heights=[]
       i=0
       while i < len(heights):
           if (i % 2 == 0):
              hrs_heights.append(heights[i])
           else:
              rss_heights.append(heights[i])
           i+=1
       j=0
       while j < len(rss_heights):
           if j+1 < len(rss_heights):
               a = rss_heights[j+1:]
               rest = sum(a)
           else:
               rest = 0
           ax.text(0, rss_heights[j]+rest-25, \
                    str(round(rss_heights[j],1)), fontsize=12, horizontalalignment='center',
                        color='black', fontweight='bold')
           j+=1
       ax.text(0, sum(rss_heights)+5, \
                   str(round(sum(rss_heights),1))+' (total)', fontsize=14, horizontalalignment='center',
                        color='black', fontweight='bold')
       k=0
       while k < len(hrs_heights):
           if k+1 < len(hrs_heights):
               b = hrs_heights[k+1:]
               rest = sum(b)
           else:
               rest = 0
           ax.text(1, hrs_heights[k]+rest-25, \
                    str(round(hrs_heights[k],1)), fontsize=12, horizontalalignment='center',
                        color='black', fontweight='bold')
           k+=1
       ax.text(1, sum(hrs_heights)+5, \
                   str(round(sum(hrs_heights),1))+' (total)', fontsize=14, horizontalalignment='center',
                        color='black', fontweight='bold')

       #plot appearance
       ax.set_ylabel("Time (s)", fontweight='bold')
       ax.set_yticks(np.arange(0,1250,50))
       ax.set_xticklabels(['RSS', 'HRS'], rotation='horizontal', fontweight='bold')
       ax.set_title('Overhead Statistics for %s to %s' % (sdate,edate),fontweight='bold')
       ax.legend(loc=0, fontsize=10)
       pdf.savefig() # saves the current figure into a pdf page
       plt.show()
       plt.close()
