#!/usr/bin/env python
"""The main database structure in grabba

"""
import os
import sqlite3 as lite



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
                   "or ( end_date => date('now') ) ;"
        self.cursor.execute ( sql_string )
        rows = self.cursor.fetchall()
        sites = [ r for r in rows ]
        sites_to_get = {}
        for sensor in self.sensors:
            for site in sites:
                sql_string = "SELECT * from %s where site=%s by date asc limit 1 ;" \
                    % ( sensor, site )
                self.cursor.execute ( sql_string )
                rows = self.cursor.fetchall()
                
    
    def add_site ( self, sitename, location, centroid_x, centroid_y, date_from,
                  end_date, target_directory, username, modis=None,
                  landsat=True, do_sentinel2=True ):
        assert centroid_x > -180. and centroid_x <=360
        assert centroid_y > -90. and centroid_y <=90.
        if end_date is None:
            end_date = "NULL"
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
                                 date_from,  end_date,  int(do_landsat), 
                                 int(do_sentinel2), modis_products, 
                                 landsat_sensors, target_directory, username ] ))
        columns = ", ".join ( inserter.keys() )
        placeholders =  ":" + ", :".join ( inserter.keys())
        with self.con:
            sql = "INSERT into sites (%s) VALUES (%s)" % ( columns, 
                                                           placeholders )
            self.cursor.execute ( sql, inserter )
            
        
        

    def _create_tables ( self, db ):
        """Creates grabba's main tables. Takes a sqlite database"""
        try:
            con= lite.connect ( db )
            cursor = con.cursor ()
            cursor.executescript("""
            CREATE TABLE sites (
                    site varchar,
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

    db.add_site ( "nebraska", "POINT((-96.4766 41.1650))", -96.4766, 41.1650, 
                 "2008-01-01",  "2009-01-01", "/tmp/", "ucfajlg", 
                 modis=["MOD09GA/006", "MYD09GA/006"],
                 landsat=["LC8"] )
    db = None