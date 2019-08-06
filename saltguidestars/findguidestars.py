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
   sdb=mysql.mysql('sdbsandbox.cape.saao.ac.za', 'sdb_v7', 'danny', 'lemmein!', port=3306)
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

   x_max=target_radeg+(4/60)
   x_min=target_radeg-(4/60)
   y_max=target_dedeg+(4.5/60)
   y_min=target_dedeg-(4.5/60)

   stardb=mysql.mysql('sdbsandbox.cape.saao.ac.za', 'TYCHO2', 'danny', 'lemmein!', port=3306)
   selcmd='VT, RADeg, DEDeg'
   tabcmd='TYCHO2'
   logcmd='VT > 17 and VT < 15'
   logcmd+=' and RADeg between target_radeg-4 and target_radeg+4 and DEDeg between target_radeg-4.5 and target_radeg+4.5'
   logcmd+=' order by VT'
   stars=stardb.select(selcmd,tabcmd,logcmd)
   print(size(stars))

#find rectangle for probe range and ring between RSS and SCAM. Stars must be within both, at a theta > 45 deg
#between each other. Preferable to have them directly opposite. magnitude above 21 and below 9. no binaries.
