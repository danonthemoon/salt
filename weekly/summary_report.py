# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 14:58:39 2015

@author: jpk

This script produces a report of observations taken the last 7 days at SALT
and print it out on the terminal and writes it to a file.

The script runs from today and queries the sdb for data going back 7 days.

"""


global __version__
__version__ = "1.0"
__date__ = "20 March 2015"
__author__ = "Paul Kotze"
__doc__="\nSALT Observing Summary Plots Generator, version "+__version__ +'''

This script uses queries generated by the report_queries.py script and plots
the relevant data in .png format

Usage: python weekly_summary_plots.py [OPTIONS]

OPTIONS are as follows, arguments are compulsory for both long and short forms.
Example formats are shown:

    -h   --help                  Prints this help
    -s   --startdate=20141220    Sets the start date for the query
    -e   --enddate=20150214      Sets the end date for the qeury
    -d   --date=20150215         Sets the date for the query, if this option
                                 is used a start date is not required, but an
                                 interval is required
    -i   --interval=7            Set the interval, in days, to go back in
                                 history for the query
'''
__usage__='''Usage: python weekly_summary_plots.py [OPTIONS]

OPTIONS are as follows, arguments are compulsory for both long and short forms.
Example formats are shown:

    -h   --help                  Prints this help

Specifying a date range:
    -s   --startdate=20141220    Sets the start date for the query
    -e   --enddate=20150214      Sets the end date for the qeury

Specifying a date and a range in days to query in the past:
    -d   --date=20150215         Sets the date for the query, if this option
                                 is used a start date is not required, but an
                                 interval is required
    -i   --interval=7            Set the interval, in days, to go back in
                                 history for the query
'''

import os
import sys
import getopt
import pandas as pd
import pandas.io.sql as psql
import MySQLdb
import matplotlib.pyplot as pl
import numpy as np
from datetime import datetime
import report_queries as rq
import getopt


def usage():
    print __usage__
    raise SystemExit(2)

def validate_date(date_text):
    '''
    this function validate the dates provided
    '''

    try:
        date = datetime.strptime(date_text, '%Y%m%d')
    except ValueError:
        raise ValueError("Incorrect data format, date should be YYYYMMDD")

    return date

def validate_int(number):

    try:
        num = int(number)
    except ValueError:
        raise ValueError('The interval should be an integer')

    return num

def parse_commandline(argv):
    # executes if module is run from the command line

    # check whether a commandline has been specified
    if len(argv)==0:
        usage()
        sys.exit(2)
    else:
        pass

    # read command line options
    try:
        opts,args = getopt.getopt(argv,"hs:e:d:i:",
                ["help","startdate=","enddate=", "date=","interval="])
    except getopt.GetoptError, inst:
        print inst
        print 'Use --help to get a list of options'
        sys.exit(2)

    # initiate the val_flags
    s_val = False
    e_val = False
    d_val = False
    i_val = False

    # parse them to the relevant variables
    for opt, arg in opts:
        if opt in ('--help'):
            usage()
        elif opt in ('-s','--startdate'):
            s_date = validate_date(arg)
            s_val = True
        elif opt in ('-e','--enddate'):
            e_date = validate_date(arg)
            e_val = True
        elif opt in ('-d','--date'):
            d_date = validate_date(arg)
            d_val = True
        elif opt in ('-i','--interval'):
            inter = validate_int(arg)
            i_val = True
        else:
            print 'Unknown option: ' + opt
            usage()

    # check all the flags and inform the user if there are missing values
    if s_val + e_val == 2:

        # check that the end date is after the start date
        date_diff = (e_date - s_date).days

        if date_diff < 0:
            raise ValueError('The start date cannot be later than the end date')
        else:
            date = datetime.strftime(e_date, '%Y-%m-%d')
            interval = date_diff

    # check that a start AND an end date is specified
    elif s_val + e_val == 1:
        if s_val:
            raise ValueError('You have to specify an end date')
        elif e_val:
            raise ValueError('You have to specify a start date')
        else:
            pass
    else:
        pass

    if d_val + i_val == 2:
        date = datetime.strftime(d_date, '%Y-%m-%d')
        interval = inter
    # check that a date AND an interval is specified
    elif d_val + i_val == 1:
        if d_val:
            raise ValueError('You have to specify an interval with a date')
        elif i_val:
            raise ValueError('You have to specify a date with an interval')
    else:
        pass

    return date, interval

def string_header(dr):
    '''
    format the header to be printed and written to file
    '''
    s = dr.ix[0].to_string().split('\n')

    txt = '''
*************** SALT Weekly Observing Stats *****************
A report for %s to
             %s
    ''' %(s[0], s[1])

    return txt

def string_weekly_total_time_breakdown(wttb):

    # determine the percantages of time broken down in catagories
    t = pd.Series(wttb.stack(), index = wttb.stack().index)
    t.index = t.index.get_level_values(1)
    per = pd.Series(np.zeros(len(t)), index = t.index)
    per['Weather':'Science'] = t['Weather':'Science'] / t.Total * 100
    per['TimeLostToWeather': 'ScienceTime'] = per['Weather':'Science']

    # write out the string:
    txt = '''
-------------------------------------------------------------
Time Breakdown:
---------------

Science time:      {} ({:,.0f}%)
Engineering time:  {} ({:,.0f}%)
Weather:           {} ({:,.0f}%)
Problems:          {} ({:,.0f}%)
--
Total:             {}

'''.format(t.ScienceTime, per.Science,
        t.EngineeringTime, per.Engineering,
        t.TimeLostToWeather, per.Weather,
        t.TimeLostToProblems, per.Problems,
        t.NightLength)

    return txt


def string_weekly_priority_breakdown(wpb):

    # create a percentage column
    wpb['per'] = pd.Series(np.zeros(len(wpb)), index = wpb.index)
    # determine the percentage from the Time column which is in seconds
    wpb.per = (wpb.Tsec / wpb.Tsec.sum()) * 100

    txt = wpb.to_string(columns=['Priority', 'No. Blocks', 'per'],
                         index=False,
                         header=False,
                         formatters={'per':'({:,.0f}%)'.format,
                                     'Priority':'   {:>5}      '.format,
                                     'No. Blocks':'   {0:,.0f}     '.format})

    hdr = '''
-------------------------------------------------------------
Priority BreakDown:
-------------------

Priority  No. Blocks
'''

    ftr = '''
--
Total             {0:,.0f}
    '''.format(wpb['No. Blocks'].sum())

    return hdr + txt + ftr

def string_weekly_subsystem_breakdown(wsb):

    # calculate the percentage of time breakdown
    # create a new percentage column
    wsb['per'] = pd.Series(np.zeros(len(wsb)), index = wsb.index)
    # determine the percentage from the Time column which is in seconds
    wsb.per = (wsb.Time / wsb.Time.sum()) * 100

    # create a string object to be printed and written to file
    txt = wsb.to_string(columns=['SaltSubsystem', 'TotalTime', 'per'],
                        index=False,
                        header=False,
                        formatters={'SaltSubsystem':'  {:>11}   '.format,
                                    'per':'({:,.0f}%)'.format,
                                    'TotalTime':' {} '.format })
    hdr = '''
-------------------------------------------------------------
Problems Time Breakdown
---------------------

SALT Subsystem  Total Time
'''

    return hdr + txt


def print_to_screen(txt):
    '''
    this function prints the formatted string to the terminal
    '''
    ftr = '''

****************** End of Weekly Report *********************
'''
    print txt + ftr

    return

def write_to_file(dr, txt, dirname='./logs/'):
    '''
    this function writes the text to a file and names the report accorting
    to the date range specified
    '''

    filename = 'weekly_report_' + datetime.strftime(dr.StartDate[0], '%Y%m%d') + \
               '-' + datetime.strftime(dr.EndDate[0], '%Y%m%d') + '.txt'

    ftr = '''

****************** End of Weekly Report *********************
'''

    with open(dirname+filename, 'w') as f:
        f.write(txt + ftr)


if __name__=='__main__':

    # parse line arguments
    date, interval = parse_commandline(sys.argv[1:])

    # open mysql connection to the sdb
    mysql_con = MySQLdb.connect(host='sdb.cape.saao.ac.za',
                port=3306, user=os.environ['SDBUSER'],
                passwd=os.environ['SDBPASS'], db='sdb')

    # use the connection to get the required data: _d
    dr_d = rq.date_range(mysql_con, date, interval=interval)
    wpb_d = rq.weekly_priority_breakdown(mysql_con, date, interval=interval)
    wtb_d = rq.weekly_time_breakdown(mysql_con, date, interval=interval)
    wttb_d = rq.weekly_total_time_breakdown(mysql_con, date, interval=interval)
    wsb_d = rq.weekly_subsystem_breakdown(mysql_con, date, interval=interval)

    # TESTING: save the dataframes
#    dr_d.save('dr_d')
#    wpb_d.save('wpd_d')
#    wtb_d.save('wtb_d')
#    wttb_d.save('wttd_d')
#    wsb_d.save('wsb_d')


    # format the string needed to print and write to file: _t
    dr_t = string_header(dr_d)
    wpd_t = string_weekly_priority_breakdown(wpb_d)
    wttb_t = string_weekly_total_time_breakdown(wttb_d)
    wsb_t = string_weekly_subsystem_breakdown(wsb_d)

    # print the report to the terminal
    print_to_screen(dr_t + wpd_t + wttb_t + wsb_t)

    # write the report to file
    write_to_file(dr_d, dr_t + wpd_t + wttb_t + wsb_t)

    mysql_con.close()
