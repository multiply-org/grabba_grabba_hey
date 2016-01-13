import datetime
import xml.etree.cElementTree as ET
import urllib2
import base64

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

def parse_xml ( xmlfile ):
    tree = ET.ElementTree( file = xmlfile )
    # Search for all the acquired images...
    for elem in tree.iter (tag="{http://www.w3.org/2005/Atom}entry"):
        for img in elem.getchildren():
            print img.tag
            for x in img.getchildren():
                print ">>>",x

def run_query ( url, username="guest", password="guest" ):
    

    # Create an OpenerDirector with support for Basic HTTP Authentication...
    auth_handler = urllib2.HTTPBasicAuthHandler()
    auth_handler.add_password(None, "http://scihub.copernicus.eu/apihub/", 
                              username, password)

    opener = urllib2.build_opener(auth_handler)
    print "Opening..."
    return  opener.open(url).read()

if __name__ == "__main__":

    location = ( 42.8,-8.1)
    start_date = datetime.datetime (2015, 1, 1)
    end_date = None
    sensor = "S2"
    url = "https://scihub.copernicus.eu/apihub/search?q="
    """    https://scihub.copernicus.eu/apihub/search?q=footprint:%22Intersects(42.800000,-8.100000)%22%20filename:S2*%20ingestiondate:[2015-01-01T00:00:00.000Z%20TO%20NOW]"""
    if len(location) == 2:
        location_str="footprint:\\Intersects(%f,%f)" % ( location[0], location[1] )
    elif len(location) == 4:
        location_str = "footprint:\\Intersects( ( POLYGON(( " + \
            "%f %f, %f %f, %f %f, %f %f, %f %f) )" % (  
        location[0], location[1],
        location[0], location[1],
        location[0], location[1],
        location[0], location[1],
        location[0], location[1] )
    if sensor == "S1":
        sensor_str ="filename:S1*"
    elif sensor == "S2":
        sensor_str ="filename:S2*"
    time_str = "ingestiondate:[%sZ" % start_date.isoformat()
    if end_date is None:
        time_str = time_str + " TO NOW]"
    query = "%s%s %s %s" % ( url, location_str, sensor_str, time_str )
    query = "%s%s" % ( url, urllib2.quote(location_str) )
    print query
    api_url = "https://scihub.copernicus.eu/"
    api_username = "guest"
    api_password = "guest"

    auth_handler = PreemptiveBasicAuthHandler()
    auth_handler.add_password(
        realm=None, # default realm.
        uri=api_url,
        user=api_username,
        passwd=api_password)
    opener = urllib2.build_opener(auth_handler)
    urllib2.install_opener(opener)
    result = urllib2.urlopen ( query ).read()
    print result
    
    
    
    ####xmlfile = "/tmp/Sentinel-download/query_results.xml"
    ####img= parse_xml ( "/tmp/Sentinel-download/query_results.xml")
    
    ####tree = ET.ElementTree( file = xmlfile )
    ##### Search for all the acquired images...
    ####for elem in tree.iter (tag="{http://www.w3.org/2005/Atom}entry"):
        ####for img in elem.getchildren():
            ####try:
                ####print ">>>",img.attrib['href']
            ####except:
                ####print img.attrib, ">>",img.text
