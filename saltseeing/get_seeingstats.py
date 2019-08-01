"""
Created July 2019

@author Danny Sallurday

Script to generate a pdf plot of seeing stats over a given range of dates.

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
from seeingstats import getseeingstats

if __name__=='__main__':
   sdb=mysql.mysql('sdbsandbox.cape.saao.ac.za', 'sdb_v7', 'danny', 'lemmein!', port=3306)
   sdate = sys.argv[1]
   edate = sys.argv[2]
   startdate = datetime.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
   enddate = datetime.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
   date = startdate


   insidedeltas=[]
   outsidedeltas=[]
   while date <= enddate:
       obsdate = '%4i-%2s-%2s' % (date.year, str(date.month).zfill(2), str(date.day).zfill(2))
       indeltas, outdeltas = getseeingstats(sdb, obsdate)
       if len(indeltas) == 0 and len(outdeltas) == 0: continue
       insidedeltas.extend(indeltas)
       outsidedeltas.extend(outdeltas)
       date += datetime.timedelta(days=1)



   with PdfPages('seeingstats-%s-%s.pdf' % (sdate, edate)) as pdf:
       #plot histograms of seeing stats
       subplot(2,1,1)
       entries, edges, _ = plt.hist(insidedeltas,25,color='r')
       #bin_centers = 0.5 * (edges[:-1] + edges[1:])
       #errorbar(bin_centers, entries, yerr=np.std(entries), fmt='r.')
       axvline(mean(insidedeltas), color='k', linestyle='dashed', linewidth=1)
       ymin, ymax = ylim()
       text(mean(insidedeltas)+30, ymax - ymax/6, 'Mean: %i' % mean(insidedeltas), fontsize=10)
       text(400, ymax - ymax/4, 'Sigma: %i' % np.std(insidedeltas), fontsize=10)
       xticks(np.arange(0, 5.1, step=.1),fontsize=6)
       yticks(fontsize=10)
       title("Inside Seeing (%i blocks)" % insides,fontsize=10,fontweight='bold')


       subplot(2,1,2)
       entries, edges, _ = plt.hist(seeingdeltas,25,color='r')
       #bin_centers = 0.5 * (edges[:-1] + edges[1:])
       #errorbar(bin_centers, entries, yerr=np.std(entries), fmt='r.')
       axvline(mean(seeingdeltas), color='k', linestyle='dashed', linewidth=1)
       ymin, ymax = ylim()
       text(mean(seeingdeltas)+30, ymax - ymax/6, 'Mean: %i' % mean(seeingdeltas), fontsize=10)
       text(400, ymax - ymax/4, 'Sigma: %i' % np.std(seeingdeltas), fontsize=10)
       xticks(np.arange(0, 5.1, step=.1),fontsize=6)
       yticks(fontsize=10)
       title("Inside Seeing (%i blocks)" % insides,fontsize=10,fontweight='bold')



       plt.suptitle('Acquisition Statistics for %s to %s' % (sdate,edate),fontweight='bold')
       pdf.savefig() # saves the current figure into a pdf page
       plt.show()
       plt.close()
