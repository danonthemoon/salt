"""
Created August 2019

@author Danny Sallurday

Find suitable guide stars for a given target.

"""
import os
import sys, time, datetime, string
import struct
import numpy as np
import sdb_mysql as mysql


if __name__=='__main__':
   host='sdbsandbox.cape.saao.ac.za'
   sdbname='sdb_v7'
   user='danny'
   password='lemmein!'
   sdb=mysql.mysql(host, sdbname, user, password, port=3306)
   targetname = sys.argv[1]
   selcmd='Target_Id, Target_Name, RaH, RaM, RaS, DecSign, DecD, DecM, DecS'
   tabcmd='Target join TargetCoordinates using (TargetCoordinates_Id)'
   logcmd='Target_Name = %s' % targetname
   target=sdb.select(selcmd, tabcmd, logcmd)
   print(target)
   target_rah=target[2]
   target_ram=target[3]
   target_ras=target[4]
   target_decsign=target[5]
   target_decd=target[6]
   target_decm=target[7]
   target_decs=target[8]
   target_radeg=(target_rah + (target_ram/60) + (target_ras/3600))*15
   target_dedeg = (target_decd + (target_decm/60) + (target_decs/3600))*target_decsign


   # limit to the rectangle
   ra_max=target_radeg+(4/60)
   ra_min=target_radeg-(4/60)
   dec_max=target_dedeg+(4/60)
   dec_min=target_dedeg-(4/60)

   sdbname='catalogues'
   stardb=mysql.mysql(host, sdbname, user, password, port=3306)
   selcmd='VT, RADeg, DEDeg'
   tabcmd='Tycho2'
   logcmd='VT between 17 and 15'
   logcmd+=' and RADeg between %i and %i and DEDeg between %i and %i' % (ra_min, ra_max, dec_min, dec_max)
   logcmd+=' order by VT'
   stars=stardb.select(selcmd,tabcmd,logcmd)
   print(size(stars))

   starName=''
   starRAdeg=0
   starDEdeg=0
   starVT=0
   VT_max=starVT+2.5
   VT_min=starVT-2.5
   proberadius=0
   probe_ra_max=target_radeg+(proberadius/60)
   probe_ra_min=target_radeg-(proberadius/60)
   probe_dec_max=target_dedeg+(proberadius/60)
   probe_dec_min=target_dedeg-(proberadius/60)
   selcmd='VT, RADeg, DEDeg'
   tabcmd='Tycho2'
   logcmd='VT between %i and %i' % (VT_max, VT_min)
   logcmd+=' and RADeg between %i and %i and DEDeg between %i and %i' % (probe_ra_min, probe_ra_max, probe_dec_min, probe_dec_max)
   logcmd+=' and Target_Name <> %s' % starName
   neighborstars=stardb.select(selcmd,tabcmd,logcmd)
   if not len(neighborstars) == 0: print("double : bright star too close")


#find rectangle for probe range and ring between RSS and SCAM. Stars must be within both, at a theta > 45 deg
#between each other. Preferable to have them directly opposite. no binaries.
