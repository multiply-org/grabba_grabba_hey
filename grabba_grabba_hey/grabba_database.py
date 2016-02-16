#!/usr/bin/env python
"""The main database structure in grabba

"""
import datetime
import os
import sqlite3 as lite

from landsat_downloader import get_landsat_file, get_path_row
from modis_downloader import get_modis_data, lonlat_to_tile
from sentinel_downloader import download_sentinel, get_mgrs


class RequestDB ( object ):

    def __init__ ( self, db, sensors=["landsat", "modis", "sentinel2"] ):

        if not os.path.exists ( db ):
            self._create_tables ( db )

        self.con = lite.connect ( db )
        self.cursor = self.con.cursor ()
        self.sensors  = sensors
        
    def list_last_dates (self ):
        """Gets the last dates with available data for all sensors for all 
        sites, provided the site is still being downloaded (e.g. end_data is
        still current)."""
        sql_string="SELECT site from sites where ( end_date is null ) " \
                   "or ( end_date >= date('now') ) ;"
        self.cursor.execute ( sql_string )
        sites = self.cursor.fetchall()
        sites_to_get = {}
        for sensor in self.sensors:
            for site in sites:
                sql_string = "SELECT * from %s where site='%s' order by date(date) asc limit 1 ;" \
                    % ( sensor, site[0] ) # surely there must be a better way...
                self.cursor.execute ( sql_string )
                rows = self.cursor.fetchall()
                
    
    def add_site ( self, sitename, location, centroid_x, centroid_y, date_from,
                  end_date, target_directory, username, modis=None,
                  landsat=True, do_sentinel2=True ):
        
        assert centroid_x > -180. and centroid_x <=360
        assert centroid_y > -90. and centroid_y <=90.
        
        if end_date is None:
            the_end_date = "NULL"
        else:
            the_end_date = end_date.isoformat()
        
        if landsat is None:
            do_landsat = 0
            landsat_sensors = "NULL"
        else:
            do_landsat = 1
            landsat_sensors = ", ".join ( landsat )
        
        if do_sentinel2 is  None:
            do_sentinel2 = 0
        
        if modis is None:
            do_modis = 0
            modis_products = "NULL"
        else:
            do_modis = 1
            modis_products = ", ".join ( modis )
            
        inserter = dict ( zip ( ["site", "location", "centroid_x", "centroid_y",
                                 "date_from", "end_date", "do_landsat", 
                                 "do_sentinel2", "do_modis", "modis_products",
                                 "landsat_sensors", "storage_dir", "username"], 
                                [ sitename, location, centroid_x, centroid_y, 
                                 date_from.isoformat(),  the_end_date,  
                                 int(do_landsat), int(do_sentinel2), 
                                 modis_products, 
                                 landsat_sensors, target_directory, username ] ))
        k = inserter.keys()
        columns = ", ".join ( k )
        placeholders =  ":" + ", :".join ( k )
        with self.con:
            sql = "INSERT into sites (%s) VALUES (%s)" % ( columns, 
                                                           placeholders )
            
            self.cursor.execute ( sql, inserter )
        
        if do_modis:
            self.get_modis ( sitename, target_directory, centroid_x, centroid_y, 
                            modis, date_from, end_date )
        if do_landsat:
            self.get_landsat ( sitename, target_directory, centroid_x, 
                              centroid_y, landsat_sensors, date_from, end_date )
            
            
    def get_landsat ( self, site, target_directory, centroid_x, centroid_y, 
                    landsat_sensors, start_date, end_date ):
        
        save_dir = os.path.join ( target_directory, "Landsat" )
        if not os.path.exists ( save_dir ):
            os.makedirs ( save_dir )
        prs = get_path_row ( centroid_y, centroid_x )
        username = "jgomezdans"
        password = "F42H0hr8"
        if not isinstance(landsat_sensors, list):
            landsat_sensors = [landsat_sensors]
        for pr in prs:
            path, row = pr
            for sensor in landsat_sensors:
                
                files = get_landsat_file ( sensor, "%03d" %path , "%03d" % row, 
                              start_date, end_date, save_dir,
                                username, password )
                dumper = []
                for fich in files:
                    fname = fich.split("/")[-1]
                    date = datetime.datetime.strptime ( fname[9:16], "%Y%j" )
                    time = date.isoformat()
                    dumper.append ( (site, path, row, time, fich, fname, sensor ))
                    
                with self.con:
                    self.cursor.executemany ( """
        INSERT INTO landsat(site, path, row, date, full_path, granule_name, sensor)
        VALUES ( ?, ?, ?, ?, ?, ?, ? )""", dumper )
                    
        
    def get_modis ( self, site, target_directory, centroid_x, centroid_y, 
                   modis_products, date_from, end_date ):
        """Download MODIS data, and store the data in the disk, keeping a 
        record of what's available in grabba's DB.
        
        Parameters
        ------------
        site: str
            The site string
        target_directory: str
            The taret directory. Downloads will go into this, into a subdir
            called "MODIS", and another subdir with the product. 
        centroid_x: float
            The longitude of the region of interest in decimal degrees. Needed
            to figure out the tile.
        centroid_y: float
            The lattitude of the region of interest in decimal degrees. Needed
            to figure out the tile.
        modis_products: iter 
            A list of MODIS Products, with the format ``["MOD09GA/006"]``
        date_from: datetime
            The starting date for the download
        end_date: datetime
            The end date, can be None
        
        """
        modis_dir = os.path.join ( target_directory, "MODIS" ) 
        if not os.path.exists ( modis_dir ):
            os.makedirs( modis_dir )
        h, v, dummy, dummy = lonlat_to_tile ( centroid_x, centroid_y )
        tile = "h%02dv%02d" % ( h, v )
#            def get_modis_data(platform, product, tiles, output_dir, start_date,
#                   end_date=None, n_threads=5):
        
        for product in modis_products:
            product_dir = os.path.join ( modis_dir, product )
            if not os.path.exists ( product_dir ):
                os.makedirs ( product_dir )
            if product.find ("MOD") >= 0:
                platform = "MOLT"
            elif product.find ( "MYD" ) >= 0:
                platform = "MOLA"
            elif product.find ( "MCD" ) >= 0:
                platform = "MOTA"
            else:
                raise NameError, "Can't figure out whether TERRA or AQUA"
            dloaded_files = get_modis_data ( platform, product, tile, 
                                            product_dir, date_from,
                                            end_date )
            dumper = []
            for fich in dloaded_files:
                fname = fich.split("/")[-1]
                time =  datetime.datetime.strptime( fname.split(".")[1][1:], 
                                                   "%Y%j" )
                time = time.isoformat()
                dumper.append ( (site, tile, time, fich, fname, product ))

            with self.con:
                self.cursor.executemany ( """
        INSERT INTO modis(site,tile,date,full_path,granule_name,product)
        VALUES ( ?, ?, ?, ?, ?, ? )""", dumper )
                    
                    
        

    def _create_tables ( self, db ):
        """Creates grabba's main tables. Takes a sqlite database"""
        try:
            con= lite.connect ( db )
            cursor = con.cursor ()
            cursor.executescript("""
            CREATE TABLE sites (
                    site varchar unique,
                    location text,
                    centroid_x float,
                    centroid_y float,
                    date_from datetime,
                    end_date datetime,
                    do_landsat integer,
                    do_modis integer,
                    do_sentinel2 integer,
                    modis_products varchar,
                    landsat_sensors varchar,
                    storage_dir varchar,
                    username varchar
            );

            CREATE TABLE landsat (
                    site varchar,
                    path integer,
                    row integer, 
                    date datetime,
                    full_path varchar,
                    granule_name varchar,
                    sensor varchar
            );

            CREATE TABLE modis (
                    site varchar,
                    tile varchar,
                    date datetime,
                    full_path varchar,
                    granule_name varchar,
                    product varchar
            );

            CREATE TABLE sentinel2 (
                    site VARCHAR,
                    date DATETIME,
                    full_path VARCHAR,
                    granule_name VARCHAR,
                    mgrs_tile VARCHAR
            );""" )

        except lite.Error, e:
            if con:
                con.rollback()
            print "Error %s" % e.args[0] 
        finally:
            if con:
                con.close()
    def __del__ ( self ):
        self.cursor.close()

if __name__ == "__main__":
    
    db = RequestDB ( "/tmp/testme.sqlite" )
#        def add_site ( sitename, location, centroid_x, centroid_y, date_from,
#                  end_date, target_directory, username, 
    the_dir = "/storage/ucfajlg/tmp/"
    #db.add_site ( "Oleiros", "POINT(( -8.2417 43.1667))", -8.2417, 43.1667, 
                 #datetime.datetime(2016,2,10),  None, the_dir, "ucfajlg", 
                 #modis=["MOD09GA.006", "MYD09GA.006"],
                 #landsat=["LC8"] )

    db.add_site ( "Oleiros", "POINT(( -8.2417 43.1667))", -8.2417, 43.1667, 
                 datetime.datetime(2016,2,1),  None, the_dir, "ucfajlg",                  landsat=["LC8"] )

    db.add_site ( "Nebraska", "POINT((-96.4766 41.1650))", -96.4766, 41.1650, 
                 datetime.datetime(2008,01,01),  datetime.datetime(2008,01,5), 
                 the_dir, "ucfajlg", 
                 modis=["MOD09GA.006", "MYD09GA.006"],
                 landsat=["LC8"] )
    db.add_site ( "Hebei", "POINT((115.284 38.51))", 115.284, 38.51,
                 datetime.datetime(2008,11,8),  datetime.datetime(2008,11,11), 
                 the_dir, "ucfajlg", 
                 modis=["MOD09GA.006", "MYD09GA.006"],
                 landsat=["LC8"] )
    #db = None
    db.list_last_dates()