#! /usr/bin/python

#from __future__ import (absolute_import, division, print_function, unicode_literals)

import os,sys
import argparse
import glob
import traceback
import datetime as dt

import numpy as np
from astropy.io import fits

from saltsdb import SALTSdb
import dataquality as dq



# -----------------------------------------------------------

def upload_straylight(db, obsdate):
    message=''
    #find all the files ready for analysis (e.g. in /salt/data/2016/xxxx/rss/product )
    trlist = glob.glob('/salt/data/{}/{}/rss/product/m*P*.fits'.format(obsdate[0:4], obsdate[4:8]))
    for rss_img in trlist:
    	hdulist = fits.open(rss_img) 
   	hdr = hdulist[0].header
	#To measure stray light on RSS, we split up the product RSS frame into 6 regions:
	#   ____________
	#  |   |   |   |
	#  | 1 | 3 | 5 |
	#  |___|___|___|
	#  |   |   |   |
	#  | 2 | 4 | 6 |
	#  |___|___|___|

	#using [SCI] here to access the pixels...
	#note that array indexing/designation is different for astropy.io.fits
	# numbers are y1:y2, x1:x2 ; plus the indexing is from zero instead of 1 (iraf).
	# so y1 and x1 are reduced by 1 c.f. IRAF/ds9 zones
	if(hdr['PROPID'] == "ENG_STRAYLIGHT" and len(hdr['FILTER']) == 0):
		camang = hdr['CAMANG']
		date = hdr['DATE-OBS']
		scidata = hdulist['SCI'].data
		z1 = np.average(scidata[1025:2052,0:1018]) 
		z2 = np.average(scidata[0:1026,0:1018])
		z3 = np.average(scidata[1025:2052,1073:2095])
		z4 = np.average(scidata[0:1026,1073:2095])
		z5 = np.average(scidata[1025:2052,2145:3165])
		z6 = np.average(scidata[0:1026,2145:3165])
                FileName=os.path.basename(str(rss_img))
                print(rss_img)
        	FileName=str(FileName.split('p')[1]) # getting rid of the mbgxp* for filename matching.
        	print "Processing File " + FileName
		print "%s %s %.2f %.2f %.2f %.2f %.2f %.2f %.2f" % (date,rss_img,camang,z1,z2,z3,z4,z5,z6)

                # get the filedata id
                try:
		    FileData_Id=sdb.select('FileData_Id', 'FileData', 'FileName like "{}"'.format(FileName))['FileData_Id'][0]
		except Exception as e:
		    raise Exception("File %s does NOT appear to have an entry in FileData. Row NOT inserted. Please follow up!" %FileName)
       
                insert = 'FileData_Id={}, mean_z1={}, mean_z2={},mean_z3={}, mean_z4={},mean_z5={}, mean_z6={}'.format(FileData_Id,z1,z2,z3,z4,z5,z6)
                sdb.replace(insert, 'RssStrayLight')

	hdulist.close()
    return 

if __name__ == "__main__": 
    error_msg=''

    parser = argparse.ArgumentParser(description='Upload straylight measurents ot the SDB')
    parser.add_argument('--date', dest='obsdate', action='store',
                        help='Observational date for the measurement')
    parser.add_argument('-f', dest='force', action='store_const',
                        const=True, default=False,
                        help='Force the updates')
    parser.add_argument('-e', dest='email', action='store_const',
                        const=True, default=False,
                        help='Email error results')
    args = parser.parse_args()


    if args.obsdate:
       obsdate = args.obsdate
    else:
       obsdate = dt.datetime.now() - dt.timedelta(seconds=86400)
       obsdate = obsdate.strftime("%Y%m%d")

    # set up database
    sdbhost = os.environ['SDB_SALT_HOST']
    sdbname = os.environ['SDB_SALT_DB']
    user=os.environ['SDBUSER']
    password=os.environ['SDBPASS']
    sdb=SALTSdb(sdbhost, sdbname, user, password, port=3306)

    #get the file names
    try:
       upload_straylight(sdb, obsdate)
    except Exception, e:
       error_msg += obsdate + '\n' + traceback.format_exc() + str(e) + '\n\n'
    
    if error_msg: print(error_msg)
  
    if args.email and error_msg:
       mailuser = os.environ['EMAIL_USER']
       mailpass = os.environ['EMAIL_PASSWORD']
       mailserver = os.environ['EMAIL_SERVER']
       dq.send_email(error_msg, 'UPLOAD_STRAYLIGHT Error', username=mailuser, 
                     password=mailpass, to=os.environ['DQEMAILLIST'], sender = os.environ['EMAIL_SENDER'])

