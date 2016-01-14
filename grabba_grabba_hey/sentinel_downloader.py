import hashlib
import os
import datetime
import sys
import xml.etree.cElementTree as ET

import requests

#hub_url = "https://scihub.copernicus.eu/dhus/search?q="
hub_url = "https://scihub.copernicus.eu/apihub/search?q="

requests.packages.urllib3.disable_warnings()

def calculate_md5 ( afile, blocksize=65536 ):
    fp = open ( afile, "r" )
    hasher = hashlib.md5()
    buf = fp.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = fp.read(blocksize)
    return hasher.digest()

def do_query ( query, user="guest", passwd="guest" ):    
    """
    A simple function to pass a query to the Sentinel scihub website. If
    successful this function will return the XML file back for further 
    processing.
    
    query: str
        A query string, such as "https://scihub.copernicus.eu/dhus/odata/v1/"
        "Products?$orderby=IngestionDate%20desc&$top=100&$skip=100"
    Returns:
        The relevant XML file, or raises error
    """
    r = requests.get ( query, auth=(user,passwd), verify=False )
    if r.status_code == 200:
        return r.text
    else:
        raise IOError("Something went wrong! Error code %d" % r.status_code )
                      
def download_product ( source, target, user="guest", passwd="guest" ):
    """
    Download a product from the SentinelScihub site, and save it to a named
    local disk location given by ``target``.
    
    source: str
        A product fully qualified URL
    target: str 
        A filename where to download the URL specified
    """
    md5_source = source.replace ( "$value", "/Checksum/Value/$value")
    r = requests.get ( md5_source, auth=(user,passwd), verify=False )
    md5 = r.text
    print md5, md5_source
    chunks = 65536 #1048576 # 1MiB...
    while True:
        r = requests.get ( source, auth=(user,passwd), stream=True, 
                          verify=False )
        if not r.ok:
            raise IOError("Can't start download... [%s]" % source )
        file_size = int ( r.headers['content-length'] )
        print "Downloading to -> %s" % target
        print "%d bytes..." % file_size
        with open ( target, 'wb' ) as fp:
            cntr = 0
            dload = 0
            for chunk in r.iter_content ( chunk_size = chunks  ):
                if chunk:
                    cntr += 1                
                    if cntr > 10:
                        dload += cntr*chunks
                        print "\tWriting %d/%d" % ( dload, file_size )
                        sys.stdout.flush()
                        cntr = 0
                        
                    fp.write ( chunk )
                    fp.flush()
                    os.fsync( fp )
                    
        md5_file = calculate_md5 ( target )
        if md5_file == md5:
            break


def parse_xml ( xml ):
    """
    Parse an OData XML file to havest some relevant information re products 
    available and so on. It will return a list of dictionaries, with one
    dictionary per product returned from the query. Each dicionary will have a
    number of keys (see ``fields_of_interest``), as well as ``link`` and 
    ``qui
    """
    fields_of_interest = [ "filename", "identifier", "instrumentshortname", 
                          "orbitnumber", "orbitdirection", "producttype", 
                          "beginposition", "endposition" ]
    tree = ET.ElementTree( ET.fromstring(xml) )
    # Search for all the acquired images...
    granules = []
    for elem in tree.iter (tag="{http://www.w3.org/2005/Atom}entry"):
        granule = {}
        for img in elem.getchildren():
            if img.tag.find ("id") >= 0:
                granule['id'] = img.text
            if img.tag.find ("link") and img.attrib.has_key ( "href" ):
                
                if img.attrib['href'].find ("Quicklook") >= 0:
                    granule['quicklook'] = img.attrib['href']
                elif img.attrib['href'].find ("$value") >= 0:
                    granule['link'] = img.attrib['href'].replace("$value", "" )
                    
            if img.attrib.has_key("name"):
                if img.attrib['name'] in fields_of_interest:
                    granule[img.attrib['name']] = img.text
        
        granules.append ( granule )
    
    return granules
            #print img.tag, img.attrib, img.text
            #for x in img.getchildren():
                

def download_sentinel ( location, input_start_date, input_sensor, output_dir,
                       input_end_date=None, username="guest", password="guest" ):



    
    input_sensor = input_sensor.upper()
    sensor_list = ["S1", "S2" ]
    if not input_sensor in sensor_list:
        raise ValueError("Sensor can only be S1 or S2. You provided %s" 
                         % input_sensor )
    else:
        sensor_str = 'filename:%s*' % input_sensor.upper()
    
    try:
        start_date = datetime.datetime.strptime( input_start_date, 
                                                "%Y.%m.%d" ).isoformat()
    except ValueError:
        try:
            start_date = datetime.datetime.strptime( input_start_date, 
                                                    "%Y-%m-%d" ).isoformat()
        except ValueError:
                start_date = datetime.datetime.strptime( input_start_date, 
                                                        "%Y/%j" ).isoformat()
    start_date = start_date + "Z"
    
    
    if input_end_date is None:
        end_date = "NOW"
    else:
        try:
            end_date = datetime.datetime.strptime( input_end_date, 
                                                    "%Y.%m.%d" ).isoformat()
        except ValueError:
            try:
                end_date = datetime.datetime.strptime( input_end_date, 
                                                    "%Y-%m-%d" ).isoformat()
            except ValueError:
                    end_date = datetime.datetime.strptime( input_end_date, 
                                                        "%Y/%j" ).isoformat()
        

    
    if len(location) == 2:
        location_str='footprint:"Intersects(%f, %f)"' % ( location[0], location[1] )
    elif len(location) == 4:
        location_str = 'footprint:"Intersects( POLYGON(( " + \
            "%f %f, %f %f, %f %f, %f %f, %f %f) ))"' % (  
        location[0], location[0],
        location[0], location[1],
        location[1], location[1],
        location[1], location[0],
        location[0], location[0] )
    
    time_str = 'beginposition:[%s TO %s]' % ( start_date, end_date )
    
    query = "%s AND %s AND %s" % ( location_str, time_str, sensor_str )
    query = "%s%s" % ( hub_url, query  )
    #query = "%s%s" % ( hub_url, urllib2.quote(query ) )
    
    result = do_query ( query, user=username, passwd=password )
    granules = parse_xml ( result )
    
    if not os.path.exists (output_dir):
        os.mkdir ( output_dir )
    ret_files = []
    for granule in granules:
        
        download_product( granule['link'] +"$value", os.path.join ( output_dir,
                    granule['filename'].replace("SAFE", "zip") ) ) 
        ret_files.append ( os.path.join ( output_dir,
                    granule['filename'].replace("SAFE", "zip") ) ) 
        
    return granules, ret_files
        
if __name__ == "__main__":
    location = (43.3650, -8.4100)
    input_start_date = "2015.01.01"
    input_end_date = None
    
    username = "guest"
    password = "guest"
    
    input_sensor = "S2"
    
    
    output_dir = "/data/selene/ucfajlg/tmp/"
    granules, retfiles = download_sentinel ( location, input_start_date, 
                                            input_sensor, output_dir )
