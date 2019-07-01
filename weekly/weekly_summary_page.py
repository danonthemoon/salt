# -*- coding: utf-8 -*-
"""

This script produces an html page for observations taken on a given observing
date at SALT

"""

import os
import sys
import pandas as pd
import pandas.io.sql as psql
import MySQLdb
import matplotlib.pyplot as pl
import numpy as np
from datetime import datetime

import mysql
import report_queries as rq
import sdb_utils as su

import report_queries as rq

from weekly_summary_report import *
from weekly_summary_plots import *


def weekly_summary_page(obsdate, sdb, els, interval=7, dirname='./logs/'):
    """Create a weekly summary for the given observing date

    Parameters
    ----------
    obsdate: str
       Observing date in YYYYMMDD format


    sdb: ~mysql.mysql
       A connection to the sdb database

    els: ~mysql.mysql
       A connection to the els database

    interval: int
       Number of days to create the results

    dirname: str
       Default directory to write results


    """
    week_txt=''
    subsystems_list = ['BMS', 'DOME', 'TC', 'PMAS', 'SCAM', 'TCS', 'STRUCT',
                       'TPC', 'HRS', 'PFIS','Proposal', 'Operations',
                       'ELS', 'ESKOM']
    cmap = pl.cm.jet
    colour_map = cmap(np.linspace(0.0, 1.0, len(subsystems_list)))
    col_dict = {}

    for i in range(0, len(subsystems_list)):
        col_dict[subsystems_list[i]] = colour_map[i]


    # display the night information
    nid = su.getnightinfo(sdb, obsdate)
    logic = 'NightInfo_Id={}'.format(nid)
    sa = sdb.select('Surname', 'NightInfo join Investigator on SA_Id=Investigator_Id', logic)[0][0]
    so = sdb.select('Surname', 'NightInfo join SaltOperator on SO1_Id=SO_Id', logic)[0][0]
    ct = sdb.select('Surname', 'NightInfo join Investigator on CTDuty_Id=Investigator_Id', logic)[0][0]
    info_txt = """
<h2> Week Summary for {0} </h2> 
<div>
   SA: {1} <br> 
   SO: {2} <br> 
   CT: {3} <br> 
</div>""".format(obsdate, sa, so, ct)
    week_txt += info_txt


    # display the night break down
    break_txt = '<h3> Week Summary</h3>'

    dr_d = rq.date_range(sdb.db, date, interval=interval)
    wpb_d = rq.weekly_priority_breakdown(sdb.db, date, interval=interval)
    wtb_d = rq.weekly_time_breakdown(sdb.db, date, interval=interval)
    wttb_d = rq.weekly_total_time_breakdown(sdb.db, date, interval=interval)
    wsb_d = rq.weekly_subsystem_breakdown(sdb.db, date, interval=interval)
    wsbt_d = rq.weekly_subsystem_breakdown_total(sdb.db, date, interval=interval)
    wtb_d = rq.weekly_time_breakdown(sdb.db, date, interval=interval)

 
    #make the charts
    date_string = '{} - {}'.format(dr_d['StartDate'][0], dr_d['EndDate'][0])
    priority_breakdown_pie_chart(wpb_d, date_string)
    weekly_total_time_breakdown_pie_chart(wttb_d, date_string)
    weekly_subsystem_breakdown_pie_chart(wsb_d, wsbt_d, col_dict, date_string)
    weekly_time_breakdown(wtb_d, date_string)


    wttb_t = string_weekly_total_time_breakdown(wttb_d)
    break_txt +=  wttb_t.replace('\n', '<br>\n') +'\n'
    ds = date_string
    date_string = '-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])
    break_txt +=  '<img src={}></img>'.format('weekly_time_breakdown_{}.png'.format(date_string))
    break_txt +=  '<img src={}></img>'.format('weekly_total_time_breakdown_pie_chart_{}.png'.format(date_string))

    wpd_t = string_weekly_priority_breakdown(wpb_d)
    break_txt +=  wpd_t.replace('\n', '<br>\n') +'\n'
    break_txt +=  '<img src={}></img>'.format('priority_breakdown_pie_chart_{}.png'.format(date_string))


    wsb_t = string_weekly_subsystem_breakdown(wsb_d)
    break_txt +=  wsb_t.replace('\n', '<br>\n') +'\n'
    break_txt +=  '<img src={}></img>'.format('weekly_subsystem_breakdown_pie_chart_{}.png'.format(date_string))


    week_txt += break_txt

    #block_txt = block_breakdown(sdb, obsdate)
    #night_txt += block_txt


    #write the results to the output
    write_page_to_file(obsdate, week_txt, dirname)


def write_page_to_file(obsdate, txt, dirname='./logs/', prefix='week'):
    """This function writes the text to a file and names the report accorting
    to the date range specified

    Parameters
    ----------
    obsdate: str
       Observing date in YYYYMMDD format

    txt: str
       Report to right to file

    dirname: str
       Default directory to write results
    """


    header = """<html>
<head><title>SALT Night Report for {0}</title></head>
<body bgcolor="white" text="black" link="blue" vlink="blue">\n
    """.format(obsdate)

    bottom="""\n<br><center> Updated: {0} </center>
              </body> 
              </hmtl>""".format(datetime.now().strftime('%Y-%m-%d  %H:%M:%S')) 
    html_txt = header+txt + bottom
    filename = prefix+'_report_{0}.html'.format(obsdate)

    with open(dirname+filename, 'w') as f:
        f.write(html_txt)

if __name__=='__main__':

    # open mysql connection to the sdb
    mysql_con = MySQLdb.connect(host='sdb.cape.saao.ac.za',
                port=3306, user=os.environ['SDBUSER'], 
                passwd=os.environ['SDBPASS'], db='sdb')

    sdbhost='sdb.salt'
    sdbname='sdb'
    user=os.environ['SDBUSER']
    password=os.environ['SDBPASS']
    sdb=mysql.mysql(sdbhost, sdbname, user, password, port=3306)

    elshost='db.suth.saao.ac.za'
    elsname='els'
    elsuser=os.environ['ELSUSER']
    elspassword=os.environ['ELSPASS']
    els=mysql.mysql(elshost, elsname, elsuser, elspassword, port=3306)


    obsdate = sys.argv[1]
    date = '{}-{}-{}'.format(obsdate[0:4], obsdate[4:6], obsdate[6:8])
    interval=7

    # use the connection to get the required data: _d

    # TESTING: save the dataframes
    #dr_d.save('dr_d')
    #wpb_d.save('wpd_d')
    #wtb_d.save('wtb_d')
    #wttb_d.save('wttd_d')
    #wsb_d.save('wsb_d')

    #create the page
    weekly_summary_page(obsdate, sdb, els, interval=7)

