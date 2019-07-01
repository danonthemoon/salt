# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 10:06:20 2015

@author: jpk

ToDo:

*automate the subsystems check. A query that checks all the subsystems in
case things change in the future should prevent issues with the pie chart
colours.

* Need to add global variables for the password.
* Need to property parse commandline options

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




import sys
import os
import getopt
import pandas as pd
import pandas.io.sql as psql
import MySQLdb
import matplotlib.pyplot as pl
import report_queries as rq
import numpy as np
import matplotlib.dates as mdates
from datetime import datetime

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

def priority_breakdown_pie_chart(x, ds, dirname='./logs/'):
    '''
    make a pie chart from the dataframe
    '''
    temp = list(x['Priority'])
    no_blocks = map(int, list(x['No. Blocks']))
    labels = ['P'+str(temp[i])+' - ' + str(no_blocks[i]) for i in range(0,len(temp))]
    values = list(x['Tsec'])

    # set colours for the priorities
    colours = ['b','c','g','m','r']

    fig = pl.figure(facecolor='w', figsize=[6, 6])
    ax = fig.add_subplot(111)
    ax.set_aspect=1

    pie_wedge_collection = ax.pie(values,
                                  colors=colours,
                                  pctdistance=0.8,
                                  radius = 0.95,
                                  autopct='%1.1f%%',
                                  textprops = {'fontsize':10,
                                               'color':'w'},
                                  wedgeprops = {'edgecolor':'white'})

    ax.legend(labels=labels, frameon=False, loc=(-0.15,0.7), fontsize=8)
    title_txt = 'Weekly Priority Breakdown - ' + str(int(x['No. Blocks'].sum())) + ' Blocks Total' + '\n {}'.format(ds)

    ax.set_title(title_txt, fontsize=12)

    filename = dirname+'priority_breakdown_pie_chart_' +'-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])+'.png'
    pl.savefig(filename, dpi=100)
#    pl.show()

def weekly_total_time_breakdown_pie_chart(x, ds, dirname='./logs/'):

    labels = ['Science - {}'.format(x['ScienceTime'][0]),
              'Engineering - {}'.format(x['EngineeringTime'][0]),
              'Weather - {}'.format(x['TimeLostToWeather'][0]),
              'Problems - {}'.format(x['TimeLostToProblems'][0])]

    values = [int(x['Science']),
              int(x['Engineering']),
              int(x['Weather']),
              int(x['Problems'])]

    colours = ['b','c','g','r']

    fig = pl.figure(facecolor='w', figsize=[6, 6])
    ax = fig.add_subplot(111)
    ax.set_aspect=1

    pie_wedge_collection = ax.pie(values,
                                  colors=colours,
                                  pctdistance=0.8,
                                  radius = 0.95,
                                  autopct='%1.1f%%',
                                  textprops = {'fontsize':10,
                                               'color':'w'},
                                  wedgeprops = {'edgecolor':'white'})

    ax.legend(labels=labels, frameon=False, loc=(-0.15,0.8), fontsize=8)

    title_txt = 'Weekly Time Breakdown - {} Total\n{}'.format(x['NightLength'][0], ds)
    ax.set_title(title_txt, fontsize=12)

    filename = 'total_time_breakdown_pie_chart_' + '-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])+'.png'
    pl.savefig(dirname+filename, dpi=100)
#    pl.show()

def weekly_subsystem_breakdown_pie_chart(x, y, col_dict, ds, dirname='./logs/'):


    subsystem = list(x['SaltSubsystem'])
    time = list(x['TotalTime'])

    labels = [subsystem[i] + ' - ' + time[i] for i in range(0,len(subsystem))]
    values = list(x['Time'])

    colours = [col_dict[i] for i in subsystem]

    fig = pl.figure(facecolor='w', figsize=[6, 6])
    ax = fig.add_subplot(111)
    ax.set_aspect=0.8

    pie_wedge_collection = ax.pie(values,
                                  colors=colours,
                                  pctdistance=0.8,
                                  radius = 0.9,
                                  autopct='%1.1f%%',
                                  textprops = {'fontsize':10,
                                               'color':'k'},
                                  wedgeprops = {'edgecolor':'white'})

    ax.legend(labels=labels, frameon=False, loc=(-0.15,0.5), fontsize=8)

    title_txt = 'Weekly Problems Breakdown - {}\n{}'.format(y['TotalTime'][0], ds)
    ax.set_title(title_txt, fontsize=12)

    filename = 'subsystem_breakdown_pie_chart_'+'-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])+'.png'
    pl.savefig(dirname+filename, dpi=100)
#    pl.show()

def weekly_time_breakdown(x, ds, dirname='./logs/'):
    '''
    produce a bar stacked bar chart plot of the time breakdown per day for the
    past week.
    '''

    fig = pl.figure(figsize=(10,4),facecolor='w')
    ax = fig.add_subplot(111)
    width = 0.65
    ax.grid(which='major', axis='y')
    # science time per day
    s = ax.bar(x['Date'],
                x['Science'],
                width,
                color = 'b',
                edgecolor='w')

    # engineering time per day
    e = ax.bar(x['Date'],
               x['Engineering'],
                width,
                bottom = x['Science'],
                color = 'c',
                edgecolor='w')

    # weather time per day
    w = ax.bar(x['Date'],
               x['Weather'],
                width,
                bottom = x['Science'] + x['Engineering'],
                color = 'g',
                edgecolor='w')

    # problem time per day
    p = ax.bar(x['Date'],
               x['Problems'],
                width,
                bottom = x['Science'] + x['Engineering'] + x['Weather'],
                color = 'r',
                edgecolor='w')


    ax.set_ylabel('Hours', fontsize=11)
    ax.set_xlabel('Date', fontsize=11)
    fig.legend((s[0], e[0], w[0], p[0]),
                   ('Science Time',
                    'Engineering Time',
                    'Time lost to Weather',
                    'Time lost to Problems'),
                    frameon=False,
                    fontsize=10,
                    loc=(0.0,0.70))

    title_txt = 'Weekly Time Breakdown - {}'.format(ds)

    ax.set_title(title_txt, fontsize=11)
    ax.xaxis_date()
    date_formatter = mdates.DateFormatter('%a \n %Y-%m-%d')
    ax.xaxis.set_major_formatter(date_formatter)

    for tick in ax.xaxis.get_major_ticks():
                tick.label.set_fontsize(8)
    for tick in ax.yaxis.get_major_ticks():
                tick.label.set_fontsize(8)

    fig.autofmt_xdate(rotation=0, ha = 'left')
    fig.subplots_adjust(left=0.22, bottom=0.20, right=0.96, top=None,
                      wspace=None, hspace=None)
    pl.autoscale()
    filename = 'time_breakdown_'+'-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])+'.png'
    pl.savefig(dirname+filename, dpi=100)
#    pl.show()


if __name__=='__main__':

    # set the colours for all the subsystems:
    subsystems_list = ['BMS', 'DOME', 'TC', 'PMAS', 'SCAM', 'TCS', 'STRUCT',
                       'TPC', 'HRS', 'PFIS','Proposal', 'Operations',
                       'ELS', 'ESKOM']
    cmap = pl.cm.jet
    colour_map = cmap(np.linspace(0.0, 1.0, len(subsystems_list)))
    col_dict = {}

    for i in range(0, len(subsystems_list)):
        col_dict[subsystems_list[i]] = colour_map[i]


    # open mysql connection to the sdb
    mysql_con = MySQLdb.connect(host='sdb.cape.saao.ac.za',
                port=3306,user=os.environ['SDBUSER'],
                passwd=os.environ['SDBPASS'], db='sdb')

#    obsdate = sys.argv[1]
#    date = '{}-{}-{}'.format(obsdate[0:4], obsdate[4:6], obsdate[6:8])
#    interval = sys.argv[2]

    date, interval = parse_commandline(sys.argv[1:])

    # use the connection to get the required data: _d
    dr_d = rq.date_range(mysql_con, date, interval=interval)
    wpb_d = rq.weekly_priority_breakdown(mysql_con, date, interval=interval)
    wtb_d = rq.weekly_time_breakdown(mysql_con, date, interval=interval)
    wttb_d = rq.weekly_total_time_breakdown(mysql_con, date, interval=interval)
    wsb_d = rq.weekly_subsystem_breakdown(mysql_con, date, interval=interval)
    wsbt_d = rq.weekly_subsystem_breakdown_total(mysql_con, date, interval=interval)
    wtb_d = rq.weekly_time_breakdown(mysql_con, date, interval=interval)

    date_string = '{} - {}'.format(dr_d['StartDate'][0], dr_d['EndDate'][0])

    # testing the pie_chart method
    priority_breakdown_pie_chart(wpb_d, date_string,'')
    weekly_total_time_breakdown_pie_chart(wttb_d, date_string,'')
    weekly_subsystem_breakdown_pie_chart(wsb_d, wsbt_d, col_dict, date_string,'')
    weekly_time_breakdown(wtb_d, date_string,'')

    mysql_con.close()

