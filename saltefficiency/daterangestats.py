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
from pylab import *
plt.switch_backend('agg')

import run_overheadstats
from nightstats import getnightstats

def subplotter(stat):
    """Plot the histograms of stat 
    """
    plt.hist(stat,26,range=(0,780),color='r')
    axvline(median(stat), color='k', linestyle='dashed', linewidth=1)
    axvline(mean(stat), color='b', linestyle='dashdot', linewidth=1)
    ymin, ymax = ylim()
    text(500, ymax - ymax/6, 'Median: %i' % median(stat), fontsize=10)
    text(500, ymax - ymax/4, 'Mean: %i' % mean(stat), color='b', fontsize=10)
    xticks(np.arange(0, 780, step=60),fontsize=6)
    yticks(fontsize=10)
    return None

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
   mos_acqtimes=[]

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
          rssblocks+=rsscount

          hrs_slewtimes.extend(nightstats[4])
          hrs_trslewtimes.extend(nightstats[5])
          hrs_targetacqtimes.extend(nightstats[6])
          hrs_instracqtimes.extend(nightstats[7])
          hrsblocks+=hrscount

          mos_slewtimes.extend(nightstats[8])
          mos_trslewtimes.extend(nightstats[9])
          mos_acqtimes.extend(nightstats[10])
          mosblocks+=moscount

          nights+=1

   if nights == 0:
       print("No valid observation nights within this range. Make sure format is yyyymmdd yyyymmdd.")
   else:
       print('Data taken from %i RSS blocks, %i HRS blocks, %i MOS blocks over %i observation nights' % (rssblocks, hrsblocks, mosblocks, nights))
       rss_stats = {}
       hrs_stats = {}
       mos_stats = {}
       if not rssblocks==0:
          rss_stats.update({'1. Slew' : median(rss_slewtimes)})
          rss_stats.update({'2. Tracker Slew' : median(rss_trslewtimes)})
          rss_stats.update({'3. Target Acquisition': median(rss_targetacqtimes)})
          rss_stats.update({'4. Instrument Acquisition': median(rss_instracqtimes)})
       if not hrsblocks==0:
          hrs_stats.update({'1. Slew' : median(hrs_slewtimes)})
          hrs_stats.update({'2. Tracker Slew' : median(hrs_trslewtimes)})
          hrs_stats.update({'3. Target Acquisition': median(hrs_targetacqtimes)})
          hrs_stats.update({'4. Instrument Acquisition': median(hrs_instracqtimes)})
       if not mosblocks==0:
          mos_stats.update({'1. Slew' : median(mos_slewtimes)})
          mos_stats.update({'2. Tracker Slew' : median(mos_trslewtimes)})
          mos_stats.update({'5. MOS Acquisition': median(mos_acqtimes)})
       else:
          mos_stats.update({'1. Slew' : 0})
          mos_stats.update({'2. Tracker Slew' : 0})
          mos_stats.update({'3. Target Acquisition': 0})
          mos_stats.update({'5. MOS Acquisition': 0})


   with PdfPages('acqstats-%s-%s.pdf' % (sdate, edate)) as pdf:
       #plot histograms of RSS and HRS acquisition stats
       subplot(2,2,1)
       subplotter(rss_targetacqtimes)
       title("RSS Target Acquisition (%i blocks)" % rssblocks,fontsize=10,fontweight='bold')

       subplot(2,2,2)
       subplotter(rss_instracqtimes)
       title("RSS Science Acquisition (%i blocks)" % rssblocks,fontsize=10,fontweight='bold')

       subplot(2,2,3)
       subplotter(hrs_targetacqtimes)
       title("HRS Target Acquisition (%i blocks)" % hrsblocks,fontsize=10,fontweight='bold')

       subplot(2,2,4)
       subplotter(hrs_instracqtimes)
       title("HRS Science Acquisition (%i blocks)" % hrsblocks,fontsize=10,fontweight='bold')

       plt.suptitle('Acquisition Statistics for %s to %s' % (sdate,edate),fontweight='bold')
       pdf.savefig() # saves the current figure into a pdf page
       plt.show()
       plt.close()

   #produce a pdf with the relevant stats, separated by instrument
   with PdfPages('overheadstats-%s-%s.pdf' % (sdate, edate)) as pdf:
       #plot RSS and HRS stats as different bars
       stats = [rss_stats, hrs_stats, mos_stats]
       df = pd.concat([pd.Series(d) for d in stats], axis=1).fillna(0).T
       df.index = ['RSS Stats', 'HRS Stats', 'MOS Stats']
       ax = df.plot(kind="bar", stacked=True, figsize=(8.27,11.69))

       #label the bar splits and totals
       rsstotal = sum(list(rss_stats.values()))
       ax.text(0, rsstotal+20, str(int(rsstotal))+' (total)', fontsize=14, horizontalalignment='center', color='black', fontweight='bold')
       rslew = rss_stats.get("1. Slew")
       rtrsl = rss_stats.get("2. Tracker Slew")
       rtacq = rss_stats.get("3. Target Acquisition")
       riacq = rss_stats.get("4. Instrument Acquisition")
       rslew_height = rslew - 25
       rtrsl_height = rtrsl+rslew_height
       rtacq_height = rtacq+rtrsl_height
       riacq_height = riacq+rtacq_height
       ax.text(0, rslew_height, str(int(rslew)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')
       ax.text(0, rtrsl_height, str(int(rtrsl)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')
       ax.text(0, rtacq_height, str(int(rtacq)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')
       ax.text(0, riacq_height, str(int(riacq)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')

       hrstotal = sum(list(hrs_stats.values()))
       ax.text(1, hrstotal+20, str(int(hrstotal))+' (total)', fontsize=14, horizontalalignment='center', color='black', fontweight='bold')
       hslew = hrs_stats.get("1. Slew")
       htrsl = hrs_stats.get("2. Tracker Slew")
       htacq = hrs_stats.get("3. Target Acquisition")
       hiacq = hrs_stats.get("4. Instrument Acquisition")
       hslew_height = hslew - 25
       htrsl_height = htrsl+hslew_height
       htacq_height = htacq+htrsl_height
       hiacq_height = hiacq+htacq_height
       ax.text(1, hslew_height, str(int(hslew)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')
       ax.text(1, htrsl_height, str(int(htrsl)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')
       ax.text(1, htacq_height, str(int(htacq)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')
       ax.text(1, hiacq_height, str(int(hiacq)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')

       if not mosblocks==0:
           mostotal = sum(list(mos_stats.values()))
           ax.text(2, mostotal+20, str(int(mostotal))+' (total)', fontsize=14, horizontalalignment='center', color='black', fontweight='bold')
           mslew = mos_stats.get("1. Slew")
           mtrsl = mos_stats.get("2. Tracker Slew")
           miacq = mos_stats.get("5. MOS Acquisition")
           mslew_height = mslew - 25
           mtrsl_height = mtrsl+mslew_height
           miacq_height = miacq+mtrsl_height
           ax.text(2, mslew_height, str(int(mslew)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')
           ax.text(2, mtrsl_height, str(int(mtrsl)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')
           ax.text(2, miacq_height, str(int(miacq)), fontsize=12, horizontalalignment='center', color='black', fontweight='bold')

       #plot appearance
       ax.set_ylabel("Time (s)", fontweight='bold')
       ax.set_yticks(np.arange(0,1250,50))
       ax.set_xticklabels(['RSS\n (%i blocks)' % rssblocks, 'HRS\n (%i blocks)' % hrsblocks, \
                   'MOS\n (%i blocks)' % mosblocks], rotation='horizontal', fontweight='bold')
       ax.set_title('Overhead Statistics for %s to %s' % (sdate,edate),fontweight='bold')
       ax.legend(loc=2, fontsize=12)
       pdf.savefig() # saves the current figure into a pdf page
       plt.show()
       plt.close()
