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
   host='sdbsandbox.cape.saao.ac.za'
   sdbname='sdb_v7'
   user='danny'
   password='lemmein!'
   sdb=mysql.mysql(host, sdbname, user, password, port=3306)
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
       date += datetime.timedelta(days=1)
       if len(indeltas) == 0 and len(outdeltas) == 0: continue
       insidedeltas.extend(indeltas)
       outsidedeltas.extend(outdeltas)



   with PdfPages('seeingstats-%s-%s.pdf' % (sdate, edate)) as pdf:
       #plot histograms of seeing stats
       subplot(2,1,1)
       plt.hist(insidedeltas, 12, range=(-3,3), color='r')
       axvline(median(stat), color='k', linestyle='dashed', linewidth=1)
       axvline(mean(stat), color='b', linestyle='dashdot', linewidth=1)
       ymin, ymax = ylim()
       text(500, ymax - ymax/6, 'Median: %i' % median(stat), fontsize=10)
       text(500, ymax - ymax/4, 'Mean: %i' % mean(stat), color='b', fontsize=10)
       xticks(np.arange(0, 5.1, step=.1),fontsize=6)
       yticks(fontsize=10)
       title("Inside Seeing (%i blocks)" % insides,fontsize=10,fontweight='bold')


       subplot(2,1,2)
       plt.hist(outsidedeltas, 12, range=(-3,3), color='r')
       axvline(median(outsidedeltas), color='k', linestyle='dashed', linewidth=1)
       axvline(mean(outsidedeltas), color='b', linestyle='dashdot', linewidth=1)
       ymin, ymax = ylim()
       text(500, ymax - ymax/6, 'Median: %i' % median(outsidedeltas), fontsize=10)
       text(500, ymax - ymax/4, 'Mean: %i' % mean(outsidedeltas), color='b', fontsize=10)

       xticks(np.arange(0, 5.1, step=.1),fontsize=6)
       yticks(fontsize=10)
       title("Outside Seeing (%i blocks)" % outsides,fontsize=10,fontweight='bold')



       plt.suptitle('Seeing Statistics for %s to %s' % (sdate,edate),fontweight='bold')
       pdf.savefig() # saves the current figure into a pdf page
       plt.show()
       plt.close()
