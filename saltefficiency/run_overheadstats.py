"""
Created July 2019

@author Danny Sallurday

Script to obtain database overhead stats for a given range of observation dates

"""

import os
import sys, time, datetime, string
import struct
import numpy as np
import sdb_mysql as mysql

from overheadstats import overheadstats

if __name__=='__main__':
   sdb=mysql.mysql('sdbsandbox.cape.saao.ac.za', 'sdb_v7', 'danny', 'lemmein!', port=3306)
   sdate = sys.argv[1]
   edate=sys.argv[2]
   sdate = datetime.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
   edate = datetime.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
   date = sdate
   while date <= edate:
       obsdate = '%4i-%2s-%2s' % (date.year, str(date.month).zfill(2), str(date.day).zfill(2))
       print (obsdate)
       results=overheadstats(sdb, obsdate, update=True)
       date += datetime.timedelta(days=1)
