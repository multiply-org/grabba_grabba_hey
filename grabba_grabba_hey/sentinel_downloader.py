import os
import datetime
import xml.etree.cElementTree as ET
import urllib2
import base64
from multiprocessing.dummy import Pool 

hub_url = "https://scihub.copernicus.eu/dhus/search?q="

class PreemptiveBasicAuthHandler(urllib2.HTTPBasicAuthHandler):
    '''Preemptive basic auth.

    Instead of waiting for a 403 to then retry with the credentials,
    send the credentials if the url is handled by the password manager.
    Note: please use realm=None when calling add_password.'''
    def http_request(self, req):
        url = req.get_full_url()
        realm = None
        # this is very similar to the code from retry_http_basic_auth()
        # but returns a request object.
        user, pw = self.passwd.find_user_password(realm, url)
        if pw:
            raw = "%s:%s" % (user, pw)
            auth = 'Basic %s' % base64.b64encode(raw).strip()
            req.add_unredirected_header(self.auth_header, auth)
        return req

    https_request = http_request

def do_query ( query, api_username="guest", api_password="guest", 
              api_url="https://scihub.copernicus.eu/" ):    

    auth_handler = PreemptiveBasicAuthHandler()
    auth_handler.add_password(
        realm=None, # default realm.
        uri=api_url,
        user=api_username,
        passwd=api_password)
    opener = urllib2.build_opener(auth_handler)
    urllib2.install_opener(opener)
    result = urllib2.urlopen ( query ).read()
    return result

def download ( x, api_username="guest", api_password="guest", 
              api_url="https://scihub.copernicus.eu/" ):    
    source, target = x
    auth_handler = PreemptiveBasicAuthHandler()
    auth_handler.add_password(
        realm=None, # default realm.
        uri=api_url,
        user=api_username,
        passwd=api_password)
    opener = urllib2.build_opener(auth_handler)
    urllib2.install_opener(opener)
    print urllib2.quote(source, "/:" )
    u = urllib2.urlopen ( urllib2.quote(source, "/:" ) )
    fp = open( target, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (target, file_size)
    file_size_dl = 0
    block_sz = 8192
    while True:
        buff = u.read(block_sz)
        if not buff:
            break
        file_size_dl += len(buff)
        fp.write(buff)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8)*(len(status)+1)
        print status,

    fp.close()
    return result


def parse_xml ( xml ):
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
    query_human = "%s%s" % ( hub_url, query  )
    query = "%s%s" % ( hub_url, urllib2.quote(query ) )
    
    result = do_query ( query, api_username=username, api_password=password )
    granules = parse_xml ( result )
    
    if not os.path.exists (output_dir):
        os.mkdir ( output_dir )
    ret_files = []
    for granule in granules:
        
        download( (granule['link'] +"$value", os.path.join ( output_dir,
                    granule['filename'].replace("SAFE", "zip") ) ) )
        ret_files.append ( os.path.join ( output_dir,
                    granule['filename'].replace("SAFE", "zip") ) ) 
        
    return granules, ret_files
        
if __name__ == "__main__":
    location = (36.5333, 6.2833)
    input_start_date = "2015.01.01"
    input_end_date = None
    
    username = "guest"
    password = "guest"
    
    input_sensor = "S2"
    
    output_dir = "/data/selene/ucfajlg/tmp/"
    granules, retfiles = download_sentinel ( location, input_start_date, 
                                            input_sensor, output_dir )
