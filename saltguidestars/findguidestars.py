"""
Created August 2019

@author Danny Sallurday

For a given observation date, find suitable guide stars for RSS targets.

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
   selcmd='Target_Id, Target_Name, RaH, RaM, RaS, DecSign, DecD, DecM, DecS'
   tabcmd='Target join TargetCoordinates using (TargetCoordinates_Id)'
   targets=sdb.select(selcmd, tabcmd, '')

   for target in targets:
       target_rah=target[2]
       target_ram=target[3]
       target_ras=target[4]

       target_decsign=target[5]
       target_decd=target[6]
       target_decm=target[7]
       target_decs=target[8 ]






#find rectangle for probe range and ring between RSS and SCAM. Stars must be within both, at a theta > 45 deg
#between each other. Preferable to have them directly opposite. magnitude above 21 and below 9. no binaries.
