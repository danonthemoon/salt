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
   slewtimes=[]
   trslewtimes=[]
   rss_targetacqtimes=[]
   rss_instracqtimes=[]
   rss_scitracktimes=[]
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
          slewtimes.extend(nightstats[0])
          trslewtimes.extend(nightstats[1])
          rss_targetacqtimes.extend(nightstats[2])
          rss_instracqtimes.extend(nightstats[3])
          rss_scitracktimes.extend(nightstats[4])
          hrs_targetacqtimes.extend(nightstats[5])
          hrs_instracqtimes.extend(nightstats[6])
          hrs_scitracktimes.extend(nightstats[7])
       blocks+=numberofblocks
       nights+=1
   if nights == 0:
       print("No valid observation nights within this range")
   else:
       slewstats = {}
       slewstats.update({'SlewTime' : median(slewtimes), 'TrackerSlewTime' : median(trslewtimes)})
       rss_stats = {}
       rss_stats.update({'RSS TargetAcquisitionTime': median(rss_targetacqtimes), 'RSS InstrumentAcquisitionTime': median(rss_instracqtimes)})
       #rss_stats.update({'RSS ScienceTrackTime': median(rss_scitracktimes)})
       hrs_stats = {}
       hrs_stats.update({'HRS TargetAcquisitionTime': median(hrs_targetacqtimes), 'HRS InstrumentAcquisitionTime': median(hrs_instracqtimes)})
       #hrs_stats.update({'HRS ScienceTrackTime': median(hrs_scitracktimes)})

   #Produce a pdf with the relevant stats
   with PdfPages('blockoverheadstats-%s-%s.pdf' % (sdate, edate)) as pdf:
       stats = [slewstats, rss_stats, hrs_stats]
       df = pd.concat([pd.Series(d) for d in stats], axis=1).fillna(0).T
       df.index = ['Slew Stats', 'RSS Stats', 'HRS Stats']
       #df = pd.DataFrame([slewstats])
       ax = df.plot(kind="bar", stacked=True, figsize=(8.27,11.69))
       heights = []
       for patch in ax.patches:
           heights.insert(0, patch.get_height())
       i = 0
       while i < len(heights):
           if i+1 < len(heights):
               a = heights[i+1:]
               rest = sum(a)
           else:
               rest = 0
           ax.text(0, heights[i]+rest-25, \
                    str(round(heights[i],1)), fontsize=14, horizontalalignment='center',
                        color='white', fontweight='bold')
           i+=1
       ax.text(0, sum(heights)+5, \
                   str(round(sum(heights),1))+' (total)', fontsize=14, horizontalalignment='center',
                        color='black', fontweight='bold')
       ax.set_ylabel("Time (s)", fontweight='bold')
       ax.set_yticks(np.arange(0,1050,50))
       ax.set_xticklabels(['Average Overhead Time per Block'], rotation='horizontal', fontweight='bold')
       ax.set_title('Overhead Statistics for %s to %s' % (sdate,edate),fontweight='bold')
       ax.legend(loc=3, fontsize=11)
       pdf.savefig() # saves the current figure into a pdf page
       plt.show()
       plt.close()
