#!/usr/bin/env python
"""
SYNOPSIS

DESCRIPTION

A MODIS daily surface reflectance download tool. Uses the
recently made available OpenDAP server to download daily
MODIS reflectance data for a particular location. This
script is designed to only fetch a single pixel, but 
annual time series of both TERRA and AQUA data. The 
script works using threads, attempting to access the
server to simultaneously request different time periods.
As of writing, we don't know about the ethics of this ;-)

The data is put into an ASCII file in "UCL's BRDF" format.
The file stores the provenance of the observations, and does
some preliminary QA filtering using the state_1km band. The 
crap observations are filtered out then...

EXAMPLES

./parallel_leech.py --lat 43.4130156 --lon -8.0694678 --output="caaveiro"

AUTHOR

Jose Gomez-Dans (UCL/NCEO)
j.gomez-dans@ucl.ac.uk


"""

from multiprocessing.dummy import Pool
import datetime
import sys
import numpy as np
from pydap.client import open_url
import optparse

def do_command_line ():


    parser = optparse.OptionParser(formatter=optparse.TitledHelpFormatter(), \
        usage=globals()['__doc__'])
    parser.add_option ('-l', '--lat', action='store', dest="latitude", \
        type=float, help='Latitude in decimal degrees' )
    parser.add_option ('-g', '--lon', action='store', dest="longitude", \
        type=float, help='Longitude in decimal degrees' )
    parser.add_option('-o', '--output', action="store", dest="output_file", \
        default="grabba", type=str, help="Output file and directory" )
    parser.add_option('-b', '--begin', action="store", dest="year_start", \
        default=2003, type=int, help="Start year" )
    parser.add_option('-e', '--end', action="store", dest="year_end", \
        type=int, default=2014, help="End year" )
    parser.add_option('-p', '--pool', action="store", dest="pool_size", \
        type=int, default=10, help="Number of simultaneous threads to use" )

    (options, args) = parser.parse_args()
    return options.latitude, options.longitude, options.output_file, \
        options.year_start, options.year_end, options.pool_size


def lonlat ( lon, lat, scale_factor=2. ):
    """A simple function to calculate the MODIS tile, as well as
    pixel location for a particular longitude/latitude pair. The
    scale factor relates to the actual MODIS spatial resolution, 
    and its possible values are 1 (=1km data), 2 (=0.5km data) and
    4 (0.25km data).
    
    Parameters
    ----------
    lon: float
        The longitude. I think it has to be between -180 and 180...
    lat: float
        The latitude. +ve is N Hemisphere, -ve is S Hemisphere
    scale_factor: float
        The scale factor for the product: 1 for 1km, 2 for 0.5km etc
    
    Returns
    -------
    The H and V tiles, as well as the line and sample location in 
    MODIS array (either 1km, 500m or 250m, as indicated by 
    ``scale_factor``)
    """
    scale_factor = scale_factor*1.
    sph = 6371007.181
    ulx = -20015109.354
    uly = 10007554.677
    cell_size = 926.62543305
    tile_size = 1200*cell_size
    x = np.deg2rad ( lon )*sph*np.cos(np.deg2rad(lat))
    y = np.deg2rad ( lat)*sph
    v_tile = int ( -( y - uly)/tile_size )
    h_tile = int ( (x - ulx)/tile_size )
    line = (uly-y-v_tile*tile_size)/(cell_size/scale_factor)
    sample = ( x - ulx - h_tile*tile_size)/(cell_size/scale_factor )
    return h_tile, v_tile, int(line), int(sample)

def grab_slave ( inp, leech_query=50):
    """
    This function downloads a particular band, for a given pixel
    and time period. Returns the band name and the array with all
    the data
    
    Parameters
    -----------
    inp: iter
        An iterable object with the location (e.g. full openDAP URL),
        the band name, the starting time bin, end time and sample
        and line
    leech_query: int 
        It appears that the OpenDAP server has some issues servicing
        long queries in time, so this option restricts the queries
        to only do e.g. 50 time steps at a time. At the time of
        writing, this can go up to 100, but if you try several
        downloads in parallel, it's unstable.
        
    Returns
    --------
    The band name and a 1D array with the data (one year of data)
    """
    location, band, i0,time, sample, line = inp
    ds = open_url ( location )
    nsteps = len ( time )
    tbins = nsteps/leech_query + 1 # We get 100 bins at a go
    x = np.zeros ( nsteps )
    for tstep in xrange(tbins):
        the_end = min(leech_query,len(time) - (leech_query*(tstep)))
        x[tstep*leech_query:(tstep*leech_query + the_end)] = \
            ds[band][(i0+tstep*leech_query):(i0+tstep*leech_query+the_end), line, sample].squeeze()
    return ( band, x )

def grab_refl_data_parallel ( lon, lat, year, collection="006" ):
    """
    This function builds up a list of the required bands and
    time intervals required to download. The output of this
    list can then be used by e.g. ``grab_slave`` to download
    individual bands.
    
    TODO finish this stuff, can't be arsed to right now
    """
    htile, vtile, line, sample = lonlat ( lon, lat, scale_factor=2. )
    # Next line is needed for 1km data, such as angles, QA...
    htile, vtile, line1k, sample1k = lonlat ( lon, lat, scale_factor=1. )
    bands_hk = [ "sur_refl_b%02d_1" % i for i in xrange(1,8) ]
    bands_hk += [ "obscov_500m_1", "QC_500m_1" ]
    bands_1k = [ "SolarZenith_1", "SolarAzimuth_1", \
        "SensorZenith_1", "SensorAzimuth_1" ]
    bands_1k += [ "state_1km_1" ]

    map_struct = []
    
    plat_list = [ "MOD09GA.%s" % collection, "MYD09GA.%s" % collection ]
        

    for isens, prod in enumerate( plat_list ):
        location = "http://opendap.cr.usgs.gov/opendap/" + \
                  "hyrax/%s/h%02dv%02d.ncml" % ( prod, htile, vtile ) 
        ds = open_url( location )
        time = ds['time'][:]
        xs = (datetime.date ( year, 1, 1) - datetime.date ( 2000, 1, 1 )).days
        xt = (datetime.date ( year, 12, 31) - datetime.date ( 2000, 1, 1 )).days
        i0 = np.nonzero( time == xs )[0]
        it = np.nonzero( time == xt )[0]
        if len(i0) == 0 or len(it) == 0:
            continue
        time = time[i0:(it+1)]

        for band in bands_hk:
            map_struct.append ( [ location, band, i0,time, sample, line] )
        for band in bands_1k:
            map_struct.append ( [ location, band, i0,time, sample1k, line1k] )
    return map_struct

def grab_refl_data ( lon, lat ):
    """This is the old and sequential way of doing things. We've gone
    all parallel now..."""
    htile, vtile, line, sample = lonlat ( lon, lat, scale_factor=2. )
    # Next line is needed for 1km data, such as angles, QA...
    htile, vtile, line1k, sample1k = lonlat ( lon, lat, scale_factor=1. )
    print "Getting tile h%02dv%02d..." % (htile, vtile)
    bands_hk = [ "sur_refl_b%02d_1" % i for i in xrange(1,8) ]
    bands_hk += [ "obscov_500m_1", "QC_500m_1" ]
    bands_1k = [ "SolarZenith_1", "SolarAzimuth_1", \
        "SensorZenith_1", "SensorAzimuth_1" ]
    bands_1k += [ "state_1km_1" ]

    retrieved_data = [{}, {}]
    for isens, prod in enumerate( [ "MOD09GA.005", "MYD09GA.005"] ):
        print "Doing product %s" % prod
        ds = open_url("http://opendap.cr.usgs.gov/opendap/" + \
                "hyrax/%s/h%02dv%02d.ncml" % ( prod, htile, vtile ) )
        print "\tGetting time..."
        sys.stdout.flush()
        time = ds['time'][:]
        retrieved_data[isens]['time'] = time
        n_tbins = len(time)/100 + 1 # We get 100 bins at a go
        
        for band in bands_hk:
            print "\tDoing %s "%band, 
            sys.stdout.flush()
            retrieved_data[isens][band] = np.zeros_like(time)
            for tstep in xrange(n_tbins):
                print "*",
                sys.stdout.flush()
                retrieved_data[isens][band][tstep*100:(tstep+1)*100] = \
                    ds[band][tstep*100:(tstep+1)*100, sample, line].squeeze()
    
        for band in bands_1k:
            print "\tDoing %s "%band, 
            sys.stdout.flush()
            retrieved_data[isens][band] = np.zeros_like(time)
            for tstep in xrange(n_tbins):
                print "*",
                sys.stdout.flush()
                retrieved_data[isens][band][tstep*100:(tstep+1)*100] = \
                    ds[band][tstep*100:(tstep+1)*100, sample1k, line1k].squeeze()
    return retrieved_data

def grab_data ( year, longitude, latitude, output_file, pool_size ):
        
    print "Downloading year %d..." % year
    the_data = grab_refl_data_parallel ( longitude, latitude, year )
    
    pool = Pool( pool_size )
    results = pool.map( grab_slave, the_data)
    pool.close()
    pool.join()
    # Now add the DoY to each dataset...
    doys = []
    for dataset in the_data:
        doys.append ( np.array ( [int((datetime.date(2000,1,1)+datetime.timedelta(days=x)).strftime("%j")) for x in dataset[3]] ) )
    
    Ntime_slots = len( doys[0] ) + len ( doys[15] ) # TERRA & AQUA
    
#    out = np.zeros(    Ntime_slots, 7+4+1+1+1+1 ) # 7 bands, 4 angles,1 QA@1K, QA@HK, ObsCov, DoY
    QA_OK=np.array([8,72,136,200,1032,1288,2056,2120,2184,2248])
    
    qa_mod09 = np.logical_or.reduce([results[13][1]==x for x in QA_OK])
    qa_myd09 = np.logical_or.reduce([results[27][1]==x for x in QA_OK])
    
    output_fname = "%s_%04d.brdf" % ( output_file, year )
    print "\tSaving file to ->%s<-" % output_fname
    fp = open ( output_fname, 'w' )
    fp.write ("# DoY,Platform,SZA,SAA,VZA,VAA,B01,B02,B03,B04,B05,B06,B07,QA1K,QAHK,ObsCov\n" )
    for doy in doys[1]:
        s = None
        passer = doys[1] == doy
        if passer.sum() == 1 and qa_mod09[passer]:
            s = "%d, %d, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10d, %10d, %10.4G" % \
                ( doy, 1, results[9][1][passer], results[10][1][passer], results[11][1][passer], results[12][1][passer],
                results[0][1][passer],results[1][1][passer],results[2][1][passer],results[3][1][passer],\
                results[4][1][passer],results[5][1][passer],results[6][1][passer], results[13][1][passer],
                results[8][1][passer],results[7][1][passer] )

            fp.write ( "%s\n" % s )

        passer = doys[14] == doy
        if passer.sum() == 1 and qa_myd09[passer]:
            s = "%d, %d, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10d, %10d, %10.4G" % \
                ( doy, 2,results[23][1][passer], results[24][1][passer], results[25][1][passer], results[26][1][passer],
                results[14][1][passer],results[15][1][passer],results[16][1][passer],results[17][1][passer],\
                results[18][1][passer],results[19][1][passer],results[20][1][passer], results[27][1][passer],
                results[22][1][passer],results[21][1][passer] )
        
            fp.write ( "%s\n" % s )
    fp.close()

if __name__ == "__main__":
    latitude, longitude, output_file,year_start, year_end, \
        pool_size = do_command_line ()
    for year in xrange ( year_start, year_end + 1):
        grab_data ( year, longitude, latitude, output_file, pool_size )