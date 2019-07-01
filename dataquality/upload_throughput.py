import os
import argparse
import glob
import traceback

import mysql
import dataquality as dq


def upload_throughput(sdb, infile, force=False):
    """Upload throughput measurements to the Science Database

    Parameters
    ----------
    sdb: ~mysql.sdb 
        Connection to the Science Database

    infile: str
        Path to file to upload to the database

    force: bool
        If True, it will update the database even if an entry 
        already exists
    """

    # parse the name of the file
    tab_name, obsdate = dq.parse_filename(infile)


    # check if it is already in the table
    sel_cmd = "{}_Id, Throughput_Id".format(tab_name)
    tab_cmd = "{} join Throughput using (Throughput_Id) join NightInfo using (NightInfo_id) ".format(tab_name)
    log_cmd = " Date = '{}-{}-{}'".format(obsdate[0:4], obsdate[4:6], obsdate[6:8])
    record = sdb.select(sel_cmd, tab_cmd, log_cmd)

    if len(record) > 0 and not force: return 

    if os.path.basename(infile).startswith('Rss'):
       instr='Rss'
    elif os.path.basename(infile).startswith('Salticam'):
       instr='Salticam'
    else:
       raise ValueError("File name not recognized")
   
    # parse the file and update or insert into the database
    lines = open(infile).readlines()

    if len(lines) < 3 :
       raise ValueError("Insufficient number of lines in {}".format(infile))

    stars = lines[0].strip()
    comment = lines[1].strip().strip('\'')
    nid = sdb.select('NightInfo_Id', 'NightInfo', log_cmd)[0][0]

    #create throughput 
    try:
       tid = sdb.select('Throughput_Id','Throughput', 'NightInfo_Id={}'.format(nid))[0][0]
    except: 
       ins_cmd = "NightInfo_Id = {} , StarsUsed = '{}', Comments = '{}'".format(nid, stars, comment)
       sdb.insert(ins_cmd, 'Throughput')
       tid = sdb.select('Throughput_Id','Throughput', 'NightInfo_Id={}'.format(nid))[0][0]

    if force:
       upd_cmd = "StarsUsed = '{}', Comments = '{}'".format(stars, comment)
       sdb.update(upd_cmd, 'Throughput', 'Throughput_Id={}'.format(tid))

    # upload each of the filters
    for l in lines[2:]:
        if not l.strip(): return
        l = l.split()
        if instr == 'Rss': 
            l[0] = l[0].strip(',')
            try:
               fid = sdb.select('RssFilter_Id', 'RssFilter', 'Barcode="{}"'.format(l[0]))[0][0]
            except IndexError:
               raise ValueError('{} is not an RSS Filter'.format(l[0]))
            ins_cmd = 'RssFilter_Id={}, RssThroughputMeasurement={}'.format(fid, l[1])
            up_cmd = 'RssFilter_Id={} and Throughput_Id={}'.format(fid, tid)
        elif instr == 'Salticam':
            l[0] = l[0].strip(',')
            try:
               fid = sdb.select('SalticamFilter_Id', 'SalticamFilter', 'SalticamFilter_Name="{}"'.format(l[0]))[0][0]
            except IndexError:
               raise ValueError('{} is not an Salticam Filter'.format(l[0]))
        ins_cmd = '{}Filter_Id={}, Throughput_Id={}, {}={}'.format(instr, fid, tid, tab_name, l[1])
        if len(record)==0:
            sdb.insert(ins_cmd, tab_name)
        elif force:
            up_cmd = '{}Filter_Id={} and Throughput_Id={}'.format(instr, fid, tid)
            uid = sdb.select('{}_Id'.format(tab_name), tab_name, up_cmd)[0][0]
            sdb.update(ins_cmd, tab_name, '{}_Id={}'.format(tab_name, uid))

    
if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Upload throughput measurents ot the SDB')
    parser.add_argument('-dir', dest='throughput_dir', action='store',
                        default='/salt/logs/dataquality/throughput/', 
                        help='Directory with throughput files')
    parser.add_argument('-f', dest='force', action='store_const',
                        const=True, default=False,
                        help='Force the updates')
    parser.add_argument('-e', dest='email', action='store_const',
                        const=True, default=False,
                        help='Email error results')


    args = parser.parse_args()
    user=os.environ['SDBUSER']
    password=os.environ['SDBPASS']
    sdb=mysql.mysql(sdbhost, sdbname, user, password, port=3306)

    #get the file names
    error_msg = ''
    for infile in glob.glob(args.throughput_dir+'*.txt'):
        try:
            upload_throughput(sdb, infile, force=args.force)
        except ValueError, e:
            error_msg += infile + '\n' + traceback.format_exc() + str(e) + '\n\n'
        except IOError, e:
            error_msg += infile + '\n' + traceback.format_exc() + str(e) + '\n\n'
    
    if error_msg: print(error_msg)
  
    if email and error_msg:
       mailuser = os.environ['MAILUSER']
       mailpass = os.environ['MAILPASS']
       dq.send_email(error_msg, 'UPLOAD_TRHOUGHPUT Error', username=mailuser, 
                     password=mailpass, to=os.environ['TPUTLIST'], sender = os.environ['MAILSENDER'])
