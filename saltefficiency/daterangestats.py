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

   sdb=mysql.mysql('sdbsandbox.cape.saao.ac.za', 'sdb_v7', 'danny', 'lemmein!', port=3306)

   sdate = sys.argv[1]
   edate = sys.argv[2]
   startdate = datetime.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
   enddate = datetime.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
   date = startdate

   #accumulate all of the overhead stats and obtain median over the given range of dates
   rss_slewtimes=[]
   rss_trslewtimes=[]
   rss_targetacqtimes=[]
   rss_instracqtimes=[]
   hrs_slewtimes=[]
   hrs_trslewtimes=[]
   hrs_targetacqtimes=[]
   hrs_instracqtimes=[]
   mos_slewtimes=[]
   mos_trslewtimes=[]
   mos_targetacqtimes=[]
   mos_instracqtimes=[]
   nights=0
   rssblocks=0
   hrsblocks=0
   mosblocks=0
   while date <= enddate:
       obsdate = '%4i-%2s-%2s' % (date.year, str(date.month).zfill(2), str(date.day).zfill(2))
       nightstats, rsscount, hrscount, moscount = getnightstats(sdb, obsdate)
       date += datetime.timedelta(days=1)
       if len(nightstats) == 0 or (rsscount==0 and hrscount==0 and moscount==0): continue
       else:
          rss_slewtimes.extend(nightstats[0])
          rss_trslewtimes.extend(nightstats[1])
          rss_targetacqtimes.extend(nightstats[2])
          rss_instracqtimes.extend(nightstats[3])
          hrs_slewtimes.extend(nightstats[4])
          hrs_trslewtimes.extend(nightstats[5])
          hrs_targetacqtimes.extend(nightstats[6])
          hrs_instracqtimes.extend(nightstats[7])
          mos_slewtimes.extend(nightstats[8])
          mos_trslewtimes.extend(nightstats[9])
          mos_targetacqtimes.extend(nightstats[10])
          mos_instracqtimes.extend(nightstats[11])
       rssblocks+=rsscount
       hrsblocks+=hrscount
       mosblocks+=moscount
       nights+=1
   if nights == 0:
       print("No valid observation nights within this range. Make sure format is yyyymmdd yyyymmdd.")
   else:
       print('Data taken from %i RSS blocks, %i HRS blocks, %i MOS blocks' % (rssblocks, hrsblocks, mosblocks))
       rss_stats = {}
       hrs_stats = {}
       mos_stats = {}
       if not rssblocks==0:
          rss_stats.update({'1. Slew' : median(rss_slewtimes)})
          rss_stats.update({'2. Tracker Slew' : median(rss_trslewtimes)})
          rss_stats.update({'3. Target Acquisition': median(rss_targetacqtimes)})
          rss_stats.update({'4. Instrument Acquisition': median(rss_instracqtimes)})
       if not rssblocks==0:
          hrs_stats.update({'1. Slew' : median(hrs_slewtimes)})
          hrs_stats.update({'2. Tracker Slew' : median(hrs_trslewtimes)})
          hrs_stats.update({'3. Target Acquisition': median(hrs_targetacqtimes)})
          hrs_stats.update({'4. Instrument Acquisition': median(hrs_instracqtimes)})
       if not mosblocks==0:
          mos_stats.update({'1. Slew' : median(mos_slewtimes)})
          mos_stats.update({'2. Tracker Slew' : median(mos_trslewtimes)})
          mos_stats.update({'3. Target Acquisition': median(mos_targetacqtimes)})
          mos_stats.update({'4. Instrument Acquisition': median(mos_instracqtimes)})
       else:
          mos_stats.update({'1. Slew' : 0})
          mos_stats.update({'2. Tracker Slew' : 0})
          mos_stats.update({'3. Target Acquisition': 0})
          mos_stats.update({'4. Instrument Acquisition': 0})


   #produce a pdf with the relevant stats, separated by instrument
   with PdfPages('overheadstats-%s-%s.pdf' % (sdate, edate)) as pdf:
       #plot RSS and HRS stats as different bars
       stats = [rss_stats, hrs_stats, mos_stats]
       df = pd.concat([pd.Series(d) for d in stats], axis=1).fillna(0).T
       df.index = ['RSS Stats', 'HRS Stats', 'MOS Stats']
       ax = df.plot(kind="bar", stacked=True, figsize=(8.27,11.69))

       #label the bar splits and totals
       rsstotal = sum(rss_stats.values())
       ax.text(0, rsstotal+20, \
                str(rsstotal)+' (total)', fontsize=14, horizontalalignment='center',
                    color='black', fontweight='bold')

       hrstotal = sum(hrs_stats.values())
       ax.text(0, hrstotal+20, \
                str(hrstotal)+' (total)', fontsize=14, horizontalalignment='center',
                    color='black', fontweight='bold')

       mostotal = sum(mos_stats.values())
       ax.text(0, mostotal+20, \
                str(mostotal)+' (total)', fontsize=14, horizontalalignment='center',
                    color='black', fontweight='bold')

       rss_heights=list(rss_stats.values())
       hrs_heights=list(hrs_stats.values())
       mos_heights=list(mos_stats.values())

       #label rss values
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
       ax.text(0, sum(rss_heights)+15, \
                   str(round(sum(rss_heights),1))+' (total)', fontsize=14, horizontalalignment='center',
                        color='black', fontweight='bold')

       #label hrs values
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
       ax.text(1, sum(hrs_heights)+10, \
                   str(round(sum(hrs_heights),1))+' (total)', fontsize=14, horizontalalignment='center',
                        color='black', fontweight='bold')

       #label mos values
       m=0
       while m < len(mos_heights):
           if m+1 < len(mos_heights):
               c = mos_heights[m+1:]
               rest = sum(c)
           else:
               rest = 0
           ax.text(2, mos_heights[m]+rest-25, \
                    str(round(mos_heights[m],1)), fontsize=12, horizontalalignment='center',
                        color='black', fontweight='bold')
           m+=1
       ax.text(2, sum(mos_heights)+10, \
                   str(round(sum(mos_heights),1))+' (total)', fontsize=14, horizontalalignment='center',
                        color='black', fontweight='bold')

       #plot appearance
       ax.set_ylabel("Time (s)", fontweight='bold')
       ax.set_yticks(np.arange(0,1050,50))
       ax.set_xticklabels(['RSS\n (%i blocks)' % rssblocks, 'HRS\n (%i blocks)' % hrsblocks, \
                   'MOS\n (%i blocks)' % mosblocks], rotation='horizontal', fontweight='bold')
       ax.set_title('Overhead Statistics for %s to %s' % (sdate,edate),fontweight='bold')
       ax.legend(loc=0, fontsize=12)
       pdf.savefig() # saves the current figure into a pdf page
       plt.show()
       plt.close()
