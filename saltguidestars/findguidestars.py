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

def getguidestars(sdb, obsdate):
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
   stardb=mysql.mysql('sdbsandbox.cape.saao.ac.za', 'TYCHO2', 'danny', 'lemmein!', port=3306)

   for target in targets:
       target_rah=target[2]
       target_ram=target[3]
       target_ras=target[4]
       target_decsign=target[5]
       target_decd=target[6]
       target_decm=target[7]
       target_decs=target[8]

       target_radeg=(target_rah + (target_ram/60) + (target_ras/3600))*15
       target_dedeg = (target_decd + (target_decm/60) + (target_decs/3600))*target_decsign

       x_max=target_radeg+(4/60)
       x_min=target_radeg-(4/60)
       y_max=target_dedeg+(4.5/60)
       y_min=target_dedeg-(4.5/60)


       selcmd='VT, RADeg, DEDeg'
       tabcmd='TYCHO2'
       logcmd='VT > 17 and VT < 15'
       logcmd+=' and RADeg between target_radeg-4 and target_radeg+4 and DEDeg between target_radeg-4.5 and target_radeg+4.5'
       logcmd+=' order by VT'
       stars=stardb.select(selcmd,tabcmd,logcmd)





       if not theta > 45: continue




#find rectangle for probe range and ring between RSS and SCAM. Stars must be within both, at a theta > 45 deg
#between each other. Preferable to have them directly opposite. magnitude above 21 and below 9. no binaries.
