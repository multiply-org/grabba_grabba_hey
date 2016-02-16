#!/usr/bin/env python
"""
Download Landsat data
"""
import os
import json
import datetime
import requests
import math

BASE_URL = "http://earthexplorer.usgs.gov/download/"

def get_path_row ( lat, lon ):
    """Uses the developmentseed.org API to get the path and row from lat/lon data.
    We ought to be using this API to leech the data, is far easier, but only
    covers LDCM, not ETM+ and historical data. Note that this function returns
    a list of tuples, as a point can be in different path/row combinations.
    """

    query = ("upperLeftCornerLatitude:[%s+TO+1000]+AND+lowerRightCornerLatitude:[-1000+TO+%s]"
                    '+AND+lowerLeftCornerLongitude:[-1000+TO+%s]+AND+upperRightCornerLongitude:[%s+TO+1000]'
                    % (lat, lat, lon, lon))
    url = "https://api.developmentseed.org/landsat/?search=%s&limit=50" % query
    r = requests.get ( url )
    dd = json.loads ( r.text )
    prs = list ( set ( [ ( x['path'], x['row'] ) for x in dd['results'] ] ) )
    return prs
    


def cycle_day (path):
    """ provides the day in cycle given the path number
    """
    cycle_day_path1  = 5
    cycle_day_increment = 7
    nb_days_after_day1 = cycle_day_path1 + cycle_day_increment*(path-1)
 
    cycle_day_path = math.fmod(nb_days_after_day1,16)
    if path >= 98: #change date line
        cycle_day_path += 1

    return cycle_day_path



def next_overpass (date1, path, sat):
    """ provides the next overpass for path after date1
    """
    date0_L5 = datetime.datetime(1985,5,4)
    date0_L7 = datetime.datetime(1999,1,11)
    date0_L8 = datetime.datetime(2013,5,1)
    if sat == 'LT5':
        date0 = date0_L5
    elif sat == 'LE7':
        date0 = date0_L7
    elif sat == 'LC8':
        date0 = date0_L8
    next_day=math.fmod((date1-date0).days-cycle_day(path)+1,16)
    if next_day != 0:
        date_overpass = date1+datetime.timedelta(16-next_day)
    else:
        date_overpass = date1
    
    return date_overpass


def get_landsat_file ( sensor, path, row, start_date, end_date, out_dir, 
                username, password ):
    
    if end_date is None:
        end_date = datetime.datetime.now()
    authentication = { "username": username, "password": password }
    filelist = []
    with requests.Session() as s:
        p = s.post( "https://ers.cr.usgs.gov/login", 
                data=authentication )
        this_date = start_date
        while this_date <= end_date:
            next_date = next_overpass ( this_date, int(path), sensor )
            this_date = next_date + datetime.timedelta ( days=1 )
            is_dload = False
            if sensor == "LC8":
                for station in [ 'LGN' ]:
                    for version in [ "00", "01", "02" ]:
                        prod_name = "%s%s%s%s%s%s" % ( sensor,path, row,
                                        next_date.strftime("%Y%j"), 
                                        station, version )
                        the_url = "%s%s/%s/STANDARD/EE" % ( BASE_URL, 
                            "4923", prod_name )
                        print "Trying %s...." % the_url
                        r = s.get ( the_url,stream=True )
                        if r.ok:
                            print "Downloading %s" % the_url
                            fname_out = os.path.join ( out_dir, 
                                                      prod_name + ".tar.gz"  )
                            with open ( fname_out, 'wb') as fp:
                                for block in r.iter_content(8192):
                                    fp.write(block)
                            is_dload = True
                            filelist.append ( fname_out )
                            print "Done!"
                            break

                    if is_dload is not True:
                        break
    return filelist

if __name__ == "__main__":
    start_date = datetime.datetime(2015,1,1)
    end_date = datetime.datetime(2016,1,1)
    username = "jgomezdans"
    password = "F42H0hr8"
    get_landsat_file ( "LC8", "193", "030", start_date, end_date, "/tmp/",
                username, password )
        