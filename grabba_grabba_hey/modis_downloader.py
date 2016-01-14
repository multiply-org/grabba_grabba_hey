#!/usr/bin/env python

import os
import urllib2
import time
import calendar
import shutil
import logging
import sys
import fnmatch

LOG = logging.getLogger( __name__ )
OUT_HDLR = logging.StreamHandler( sys.stdout )
OUT_HDLR.setFormatter( logging.Formatter( '%(asctime)s %(message)s') )
OUT_HDLR.setLevel( logging.INFO )
LOG.addHandler( OUT_HDLR )
LOG.setLevel( logging.INFO )

HEADERS = { 'User-Agent' : 'grabba_grabba_hey Python 0.0.0' }

def parse_modis_dates ( url, dates, product, out_dir, ruff=False ):
    """Parse returned MODIS dates.
    
    This function gets the dates listing for a given MODIS products, and 
    extracts the dates for when data is available. Further, it crosses these 
    dates with the required dates that the user has selected and returns the 
    intersection. Additionally, if the `ruff` flag is set, we'll check for
    files that might already be present in the system and skip them. Note
    that if a file failed in downloading, it might still be around
    incomplete.
    
    Parameters
    ----------
    url: str
        A URL such as "http://e4ftl01.cr.usgs.gov/MOTA/MCD45A1.005/"
    dates: list
        A list of dates in the required format "YYYY.MM.DD"
    product: str
        The product name, MOD09GA.005
    out_dir: str
        The output dir
    ruff: bool
        Whether to check for present files
    Returns
    -------
    A (sorted) list with the dates that will be downloaded.
    """
    if ruff:
        product = product.split(".")[0]
        already_here = fnmatch.filter ( os.listdir ( out_dir ), "%s*hdf" % product )
        already_here_dates = [ x.split(".")[-5][1:] \
            for x in already_here ]
                                      
    req = urllib2.Request ( "%s" % ( url ), None, HEADERS)
    html = urllib2.urlopen(req).readlines()
            
    available_dates = []
    for line in html:
        
        if line.find ( "href" ) >= 0 and line.find ( "[DIR]" ) >= 0:
            # Points to a directory
            the_date = line.split('href="')[1].split('"')[0].strip("/")
            
            if ruff:
                try:
                    modis_date = time.strftime( "%Y%j", time.strptime( \
                        the_date, "%Y.%m.%d") )
                except ValueError:
                    continue
                if modis_date in already_here_dates:
                    continue
                else:
                    available_dates.append ( the_date )    
            else:
                available_dates.append ( the_date )    
                
            
    
    dates = set ( dates )
    available_dates = set ( available_dates )
    suitable_dates = list( dates.intersection( available_dates ) )
    suitable_dates.sort()
    return suitable_dates
    
    
def get_modisfiles ( platform, product, year, tile, \
    doy_start=1, doy_end = -1,  \
    base_url="http://e4ftl01.cr.usgs.gov", 
    proxy=None, out_dir=".", ruff=False, verbose=False ):

    """Download MODIS products for a given tile, year & period of interest

    This function uses the `urllib2` module to download MODIS "granules" from 
    the USGS website. The approach is based on downloading the index files for
    any date of interest, and parsing the HTML (rudimentary parsing!) to search
    for the relevant filename for the tile the user is interested in. This file
    is then downloaded in the directory specified by `out_dir`.
    
    The function also checks to see if the selected remote file exists locally.
    If it does, it checks that the remote and local file sizes are identical. 
    If they are, file isn't downloaded, but if they are different, the remote 
    file is downloaded. 

    Parameters
    ----------
    platform: str
        One of three: MOLA, MOLT MOTA
    product: str
        The product name, such as MOD09GA.005 or MYD15A2.005. Note that you 
        need to specify the collection number (005 in the examples)
    year: int
        The year of interest
    tile: str
        The tile (e.g., "h17v04")
    proxy: dict
        A proxy definition, such as {'http': 'http://127.0.0.1:8080', \
        'ftp': ''}, etc.
    doy_start: int
        The starting day of the year.
    doy_end: int 
        The ending day of the year.
    base_url: str, url
        The URL to use. Shouldn't be changed, unless USGS change the server.
    out_dir: str 
        The output directory. Will be create if it doesn't exist
    ruff: Boolean
        Check to see what files are already available and download them without
        testing for file size etc.
    verbose: Boolean
        Whether to sprout lots of text out or not.

    Returns
    -------
    Nothing
    """
    
    if proxy is not None:
        proxy = urllib2.ProxyHandler( proxy )
        opener = urllib2.build_opener( proxy )
        urllib2.install_opener( opener )
    
    if not os.path.exists ( out_dir ):
        if verbose:
            LOG.info("Creating outupt dir %s" % out_dir )
        os.makedirs ( out_dir )
    if doy_end == -1:
        if calendar.isleap ( year ):
            doy_end = 367
        else:
            doy_end = 366
    
    dates = [time.strftime("%Y.%m.%d", time.strptime( "%d/%d" % ( i, year ), \
            "%j/%Y"))  for i in xrange(doy_start, doy_end )]
    url = "%s/%s/%s/" % ( base_url, platform, product )
    dates = parse_modis_dates ( url, dates, product, out_dir, ruff=ruff )
    for date in dates:
        the_day_today = time.asctime().split()[0]
        the_hour_now = int( time.asctime().split()[3].split(":")[0] )
        if the_day_today == "Wed" and 14 <= the_hour_now <= 17:
            time.sleep ( 60*60*( 18-the_hour_now) )
            LOG.info ( "Sleeping for %d hours... Yawn!" % ( 18 - the_hour_now) )
        req = urllib2.Request ( "%s/%s" % ( url, date), None, HEADERS )
        try:
            html = urllib2.urlopen(req).readlines()
            for line in html:
                if line.find( tile ) >=0  and line.find(".hdf") >= 0 and \
                    line.find(".hdf.xml") < 0:
                    fname = line.split("href=")[1].split(">")[0].strip('"')
                    req = urllib2.Request ( "%s/%s/%s" % ( url, date, fname), \
                        None, HEADERS )
                    download = False
                    if not os.path.exists ( os.path.join( out_dir, fname ) ):
                        # File not present, download
                        download = True
                    else:
                        the_remote_file = urllib2.urlopen(req)
                        remote_file_size = int ( \
                            the_remote_file.headers.dict['content-length'] )
                        local_file_size = os.path.getsize(os.path.join( \
                            out_dir, fname ) )
                        if remote_file_size != local_file_size:
                            download = True
                        
                    if download:
                        if verbose:
                            LOG.info ( "Getting %s..... " % fname )
                        with open ( os.path.join( out_dir, fname ), 'wb' ) \
                                as local_file_fp:
                            shutil.copyfileobj(urllib2.urlopen(req), \
                                local_file_fp)
                            if verbose:
                                LOG.info("Done!")
                    else:
                        if verbose:
                            LOG.info ("File %s already present. Skipping" % \
                                fname )

        except urllib2.URLError:
            LOG.info("Could not find data for %s(%s) for %s" % ( product, \
                platform, date ))
    if verbose:
        LOG.info("Completely finished downlading all there was")
        
