"""
Updated July 2019 by Danny Sallurday

For a given block visit, calculate:

Slew Time (Point Command - Track Start)
Tracker Slew Time (Track Start - On Target)
Target Acquisition Time (On Target - First Salticam Image)
Instrument Acquisition Time (First Salticam Image - First Science Image)
Science Track Time (First Science Image - Track End)

Then update these values in the BlockVisit table according to BlockVisit_Id

"""
import sys
import time
import datetime
import string
import numpy as np
import mysql

def getnightinfo(sdb, obsdate):
    return sdb.select('NightInfo_Id', 'NightInfo', 'Date=\'%s\'' % obsdate)[0][0]


def blockvisitstats(sdb, obsdate, update=True):
   """Determine the block visit statistics for an observation date.  These
      statistics include slew time, acquisition time, and total science
      time for the block.   For rejected blocks, this includes the time
      until the next pointing

      Parameters
      ----------

      sdb: mysql-instance
           sdb is a connection to the science data base
      obsdate: string
           observation date of interest

   """
   bvs_updated = 0
   #for a given obsdate get the night info
   nid=getnightinfo(sdb, obsdate)

   #get the times for the night
   record=sdb.select('EveningTwilightEnd, MorningTwilightStart', 'NightInfo', 'NightInfo_Id=%i' % nid)
   stime=record[0][0]
   etime=record[0][1]
   totaltime=(etime-stime).seconds

   #From the sdb, get the SoLogEvent table
   record=sdb.select('EventType_Id, EventTime', 'SoLogEvent', 'NightInfo_Id=%i' % nid)
   event_list=[]
   for i in range(len(record)):
       r=record[i]
       if r[1].seconds>43200:
          t=datetime.datetime(int(obsdate[0:4]), int(obsdate[5:7]), int(obsdate[8:10]), 0, 0, 0)+r[1]
       else:
          t=datetime.datetime(int(obsdate[0:4]), int(obsdate[5:7]), int(obsdate[8:10]), 0, 0, 0)+datetime.timedelta(days=1)+r[1]
       event_list.append([r[0], t])

   #sort the list by the datetimes
   event_list.sort(key=lambda e:e[1])

   #get the night's list of blocks
   selcmd='BlockVisit_Id, BlockVisitStatus_Id, Proposal_Code, Block_Id'
   tabcmd='Block join BlockVisit using (Block_Id) join Proposal using (Proposal_Id) join ProposalCode on (Proposal.ProposalCode_Id = ProposalCode.ProposalCode_Id)'
   blocks=sdb.select(selcmd, tabcmd, 'NightInfo_Id=%i' % nid)
   blocks=list(blocks)
   #print(blocks)


   #list of accepted blocks
   bvid_list=[]
   rej_list=[]
   for b in blocks:
       if b[1]==1:
          bvid_list.append(b[0])
       else:
          rej_list.append(b[0])
    #list of accepted blocks
   pid_list=[]
   rej_list=[]
   for b in blocks:
      if b[1]==1:
         pid_list.append(b[2])
      else:
         rej_list.append(b[2])

   #print('bvids: ', bvid_list)

   #get a list of all data from the night
   select_state='FileName, Proposal_Code, Target_Name, ExposureTime, UTSTART, h.INSTRUME, h.OBSMODE, h.DETMODE, h.CCDTYPE, NExposures, BlockVisit_Id'
   table_state='FileData  Join ProposalCode on (FileData.ProposalCode_Id = ProposalCode.ProposalCode_Id) join FitsHeaderImage as h using (FileData_Id)'
   formatteddate = obsdate.replace('-','')
   logic_state="FileName like '%"+formatteddate+"%' order by UTSTART"
   img_list=sdb.select(select_state, table_state, logic_state)
   img_list[:] = [img for img in img_list if not "CAL_" in img[1] and not "ENG_" in img[1] and not "JUNK" in img[1]]

   #print("imgs: " , img_list)
   #get a list of all point commands from the night
   select_state= 'BlockVisit_Id, EventTime, Block_Id, Target_Name, NightInfo_Id, EventData'
   table_state='PointEvent join SoLogEvent using (SoLogEvent_Id)'
   plist=sdb.select(select_state, table_state, 'NightInfo_Id=%i' % nid)
   point_list=[]
   for i in range(len(plist)):
       p=plist[i]
       if p[1].seconds>43200:
          t=datetime.datetime(int(obsdate[0:4]), int(obsdate[5:7]), int(obsdate[8:10]), 0, 0, 0)+p[1]
       else:
          t=datetime.datetime(int(obsdate[0:4]), int(obsdate[5:7]), int(obsdate[8:10]), 0, 0, 0)+datetime.timedelta(days=1)+p[1]
       point_list.append([p[0], t, p[2], p[3], p[4], p[5]])

   #print("points: ", point_list)
   #deal with accepted blocks
   block_list=[]
   for bvid in bvid_list:

      #determine start time (point) and end time (track end)
       pointtime = findpointcommand(bvid, point_list)
       if pointtime is None:
           print('no point')
           continue
       starttime=pointtime
       #print(starttime)
       endtime=findguidingstop(starttime, event_list)
       if endtime is None:
           print('no end')
           continue

       #determine total time
       tottime=endtime-starttime
       #some limit to avoid crazy stats
       if tottime.seconds > 10000:
           print('total too long')
           continue

       propcode, target, bid, instr, obsmode, detmode, exptime, nexposure = finddata(img_list, starttime, endtime)
       if propcode in pid_list and not (propcode in rej_list):
           blocks = removepropcode(blocks, propcode)
           block_list.append([bvid, starttime, endtime, 0, propcode])
       elif propcode in rej_list and not (propcode in pid_list):
           status = getblockrejectreason(sdb, propcode, blocks)
           block_list.append([bvid, starttime, endtime, status, propcode])
       elif propcode in pid_list and propcode in rej_list:
           #get the first block with the propcode
           for b in blocks:
               if b[0]==bvid:
                  if b[1]==1:
                     status=0
                  else:
                     status = getblockrejectreason(sdb, propcode, blocks)
                  block_list.append([bvid, starttime, endtime, status, propcode])
                  blocks = removepropcode(blocks, propcode)

       #determine the slew time
       guidestart=findguidingstart(starttime, event_list)
       if guidestart is None:
           print('no trackstart')
           continue
       slewtime=guidestart-starttime
       if slewtime.seconds > 1200:
           print('slew too long')
           continue

       #determine the time between TrackStart and OnTarget
       ontarget=findontarget(starttime, event_list)
       if ontarget is None:
           print('no acq')
           continue
       trackerslewtime=ontarget-guidestart
       if trackerslewtime.seconds > 300:
           print('trackslew too long')
           continue


       #determine the acquisition time after being on target
       instr, primary_mode=getprimarymode(img_list, bvid)
       #print(instr, primary_mode)

       select_state= 'Block_Id'
       table_state='BlockVisit'
       logic_state='BlockVisit_Id=%i' % (bvid)
       bids=sdb.select(select_state, table_state, logic_state)
       if not bids:
           continue
       else:
           bid=bids[0]
           selcmd='Block_Id, Barcode'
           tabcmd='Block join Pointing using (Block_Id) join Observation using (Pointing_Id) '
           tabcmd+='join TelescopeConfigObsConfig using (Pointing_Id) join ObsConfig on (PlannedObsConfig_Id=ObsConfig_Id) '
           tabcmd+='join RssPatternDetail using (RssPattern_Id) join Rss using (Rss_Id) join RssProcedure using (RssProcedure_Id) '
           tabcmd+='join RssConfig using (RssConfig_Id) join RssMask using (RssMask_Id)'
           logcmd='RssProcedureType_Id = \'7\' and Block_Id = %i group by Block_Id order by Block_Id' % bid
           mos=sdb.select(selcmd, tabcmd, logcmd)
           if mos:
               instr='MOS'

       if instr == 'MOS':
           mosacq=getfirstimage(img_list, ontarget-datetime.timedelta(seconds=2*3600.0), instr, primary_mode, bvid)
           mosacqtime=mosacq-ontarget
           if mosacqtime.seconds > 1000: continue
       else:
          scamstart=getfirstscam(img_list, starttime, 'SALTICAM', 'IMAGING', bvid)
          if scamstart is None:
             print("Did not find SCAM image")
             continue
          acqtime=scamstart-ontarget
          if acqtime.seconds > 1000: continue

       #determine the time between acquisition and first science image
       if not instr == 'MOS':
          sciencestart=getfirstimage(img_list, scamstart-datetime.timedelta(seconds=2*3600.0), instr, primary_mode, bvid)
          if sciencestart is None:
          print("Did not find science image")
          continue
          sciacqtime=sciencestart-scamstart
          if sciacqtime.seconds > 1000: continue
       #print("scam: ", scamstart, "sci: ", sciencestart)
       #print('sciacqti: ', sciacqtime)

       #update results in sdb
       if update:
           bvs_updated+=1
           #inscmd='TotalSlewTime=%i, TotalAcquisitionTime=%i, TotalScienceTime=%i' % (slewtime.seconds+trackerslewtime.seconds, acqtime.seconds, scitime.seconds)
           #sdb.update(inscmd, 'BlockVisit', 'BlockVisit_Id=%i' % bvid)
           inscmd='SlewTime=%i, TrackerSlewTime=%i, TargetAcquisitionTime=%i' % (slewtime.seconds, trackerslewtime.seconds, acqtime.seconds)
           sdb.update(inscmd, 'BlockVisit', 'BlockVisit_Id=%i' % bvid)
           inscmd='InstrumentAcquisitionTime=%i' % (sciacqtime.seconds)
           sdb.update(inscmd, 'BlockVisit', 'BlockVisit_Id=%i' % bvid)
           if instr == 'MOS':
               inscmd='MOSAcquisitionTime=%i' % (mosacqtime.seconds)
               sdb.update(inscmd, 'BlockVisit', 'BlockVisit_Id=%i' % bvid)


   print(bvs_updated)
   print(len(bvid_list))
   return block_list

def removepropcode(blocks, propcode):
    for b in blocks:
        if b[2]==propcode:
           blocks.remove(b)
           return blocks
    return blocks

def get_blockvisitfrompointtime(sdb, starttime, propcode=None):
    table = 'PointEvent join SoLogEvent using (SoLogEvent_Id)'
    logic = 'EventTime="{}"'.format(starttime)
    if propcode is not None and propcode!='JUNK' and not propcode.count("CAL_") and not propcode.count("ENG_"):
        logic += ' and Proposal_Code="{}"'.format(propcode)
   # print sdb.select('BlockVisit_Id', table, logic)
   # print starttime, propcode
    try:
        bvid = sdb.select('BlockVisit_Id', table, logic)[0][0]
    except IndexError:
        bvid = 0
    return bvid

def getblockrejectreason(sdb, propcode, blocks):
    """Get the reason for the block rejection"""
    for b in blocks:
        if propcode == b[2]:
           record = sdb.select('BlockRejectedReason_Id', 'BlockVisit', 'BlockVisit_Id=%i' % b[0])[0][0]
           return record
    return 0

def getblockvisit(blocks, bid, accept=1):
    for b in blocks:
        if b[3]==bid and b[1]==accept: return b[0]
    return None

def getfirstscam(image_list, starttime, instr, primary_mode, bvid):
    """Determine the first image of a list that has that
       mode in use

    """
    stime=starttime-datetime.timedelta(seconds=2*3600.0)
    for img in image_list:
        #print(img[4], img[5],img[6],img[10])
        #print(stime, instr, primary_mode, bid)
        if img[4]>stime and img[5]==instr: #img[6]==primary_mode and img[10]==bid:
           return img[4]+datetime.timedelta(seconds=2*3600.0)
    return None

def getfirstimage(image_list, starttime, instr, primary_mode, bvid):
    """Determine the first image of a list that has that
       mode in use

    """
    stime=starttime-datetime.timedelta(seconds=2*3600.0)
    #print(stime, instr, bvid)
    '''if instr=='RSS':
        blockscams=[]
        for img in image_list:
           if img[10]==bvid and img[5]=='SALTICAM':
              blockscams.append(img)
        if len(blockscams)<2:
           print('not enough scams')
           return None'''
    if instr == 'MOS':
        for img in image_list:
            if img[4]>stime and img[5]=='RSS' and img[10]==bvid:
               return img[4]+datetime.timedelta(seconds=2*3600.0)


    for img in image_list:
        #print(img[4], img[5],img[6],img[10])
        #print(stime, instr, primary_mode, bid)
        if img[4]>stime and img[5]==instr and img[10]==bvid: #img[6]==primary_mode and img[10]==bid:
           return img[4]+datetime.timedelta(seconds=2*3600.0)
    return None


def getprimarymode(image_list, bvid):
   """Determine the primary mode of the science frame for the block

   """
   primary_mode=None
   instr=[]
   obsmode=[]
   detmode=[]
   for img in image_list:
       if img[10]==bvid:
          instr.append(img[5])
          obsmode.append(img[6])
          detmode.append(img[7])

   #set the instrument
   if 'RSS' in instr:
       instr='RSS'
   elif 'HRS' in instr:
       instr='HRS'
   else:
       instr='SALTICAM'

   #set the mode
   if instr=='RSS':
      if 'SPECTROSCOPY' in obsmode:
         primary_mode='SPECTROSCOPY'
      elif 'FABRY-PEROT' in obsmode:
         primary_mode='FABRY-PEROT'
      else:
         primary_mode='IMAGING'
   elif instr=='HRS':
      primary_mode = obsmode[0]
   elif instr=='SALTICAM':
      if 'SLOTMODE' in detmode:
         primary_mode='SLOTMODE'
      else:
         primary_mode='NORMAL'

   return instr, primary_mode

def findpointcommand(bvid, event_list):
   """The next pointing occurs either when the next point to target
      command happens
   """
   for r in event_list:
       if r[0]==bvid:
           return r[1]
   return None

def findguidingstart(starttime, event_list):
   """Determine when guiding starts from the next guiding command
      in the event list
   """
   for r in event_list:
       if r[0]==5 and r[1]>starttime: return r[1]
   return None

def findguidingstop(starttime, event_list):
   """Determine when guiding stops from the next guiding command
      in the event list
   """
   for r in event_list:
       #print(r[1], type(r[1]), starttime, type(starttime))
       if r[0]==6 and r[1]+datetime.timedelta(seconds=0)>starttime: return r[1]
   return None

def findontarget(starttime, event_list):
   """Determine when the guider first gets on target
   """
   for r in event_list:
       if r[0]==18 and r[1]>starttime: return r[1]
   return None

def finddata(img_list, starttime, endtime):
    """Determine if any data were taken between
       start time and endtime
    """
    #convert to UT
    stime=starttime-datetime.timedelta(seconds=2*3600.0)
    etime=endtime-datetime.timedelta(seconds=2*3600.0)
    for img in img_list:
       if img[4] is not None:
           if img[4]>stime and img[4]<etime and img[2] not in ['ARC', 'FLAT']:
               return img[1], img[2], img[10], img[5], img[6], img[7], img[3], img[9]

    return [None]*8

def findnextpoint(starttime, record, etime=None):
   """The next pointing occurs either when the next point to target
      command happens
   """
   for r in record:
       if r[0]==3 and r[1]>starttime: return r[1]
   return etime
