"""
Created July 2019

@author Danny Sallurday

For a given observation date, track inside and outside seeing stats for each block.

"""
import os
import sys, time, datetime, string
import struct
import numpy as np
import sdb_mysql as mysql

def getnightinfo(sdb, obsdate):
    return sdb.select('NightInfo_Id', 'NightInfo', 'Date=\'%s\'' % obsdate)[0][0]

def getseeingstats(sdb, obsdate):
   """Determine the overhead statistics for a specific observation date.

       Parameters
       ----------
       sdb: ~mysql.mysql
          A connection to the sdb database
       obsdate: str
          Observing date in YYYYMMDD format
   """

   #get the NightInfo_Id for the given obsdate
   nid=getnightinfo(sdb, obsdate)
