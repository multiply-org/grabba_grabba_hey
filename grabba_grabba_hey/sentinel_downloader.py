#!/usr/bin/env python
"""
A simple interface to download Sentinel-1 and Sentinel-2 datasets from
the COPERNICUS Sentinel Hub.
"""
import hashlib
import datetime
from functools import partial
import logging
import os
import shutil
import re
import sys
import time
import xml.etree.cElementTree as ET

from concurrent import futures

import requests

logging.basicConfig(level=logging.INFO)

# not so much to use basicConfig as a quick usage of %(pathname)s
logging.basicConfig(level=logging.DEBUG,
                    format='%(funcName)s %(asctime)s %(levelname)s %(message)s')
LOG = logging.getLogger(__name__)


logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

# hub_url = "https://scihub.copernicus.eu/dhus/search?q="
hub_url = "https://scihub.copernicus.eu/apihub/search?q="
MGRS_CONVERT = "http://legallandconverter.com/cgi-bin/shopmgrs3.cgi"
aws_url = 'http://sentinel-s2-l1c.s3.amazonaws.com/?delimiter=/&prefix=tiles/'
aws_url_dload = 'http://sentinel-s2-l1c.s3.amazonaws.com/'
requests.packages.urllib3.disable_warnings()


def get_mgrs(longitude, latitude):
    """A method that uses a website to infer the Military Grid Reference System
    tile that is used by the Amazon data buckets from the latitude/longitude

    Parameters
    -------------
    longitude: float
        The longitude in decimal degrees
    latitude: float
        The latitude in decimal degrees
    Returns
    --------
    The MGRS tile (e.g. 29TNJ)
    """
    r = requests.post(MGRS_CONVERT,
                      data=dict(latitude=latitude,
                                longitude=longitude, xcmd="Calc", cmd="gps"))
    for liner in r.text.split("\n"):
        if liner.find("<title>") >= 0:
            mgrs_tile = liner.replace("<title>", "").replace("</title>", "")
            mgrs_tile = mgrs_tile.replace(" ", "")
    try:
        return mgrs_tile[:5]  # This should be enough
    except NameError:
        return None


def calculate_md5(fname):
    hasher = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest().upper()


def do_query(query, user="guest", passwd="guest"):
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
    r = requests.get(query, auth=(user, passwd), verify=False)
    if r.status_code == 200:
        return r.text
    else:
        raise IOError("Something went wrong! Error code %d" % r.status_code)


def download_product(source, target, user="guest", passwd="guest"):
    """
    Download a product from the SentinelScihub site, and save it to a named
    local disk location given by ``target``.

    source: str
        A product fully qualified URL
    target: str
        A filename where to download the URL specified
    """

    if os.path.exists(target):
        # File already exists on file system. Can only be that
        # it's been downloaded already and checked vs MD5 hash
        # Just return empty
        logging.info("\t{} already exists. Skipping".format(target))
        return
    chunks = 1048576  # 1MiB...
    md5_source = source.replace("$value", "/Checksum/Value/$value")
    r = requests.get(md5_source, auth=(user, passwd), verify=False)
    md5 = r.text
    # Infinite loop until we get the hash to match the official one
    while True:
        logging.debug("Getting %s" % target)
        r = requests.get(source, auth=(user, passwd), stream=True,
                         verify=False)
        if not r.ok:
            raise IOError("Can't start download... [%s]" % source)
        file_size = int(r.headers['content-length'])
        logging.info("Downloading to -> %s" % target)
        logging.info("%d bytes..." % file_size)
        with open(target+".part", 'wb') as fp:
            cntr = 0
            dload = 0
            for chunk in r.iter_content(chunk_size=chunks):
                if chunk:
                    cntr += 1
                    if cntr > 100:
                        dload += cntr * chunks
                        logging.info("\tWriting %d/%d [%5.2f %%]" %
                                     (dload, file_size,
                                      100. * float(dload) / float(file_size)))
                        sys.stdout.flush()
                        cntr = 0

                    fp.write(chunk)
                    fp.flush()
                    os.fsync(fp)
        shutil.move(target+".part", target)
        md5_file = calculate_md5(target)
        if md5_file == md5:
            logging.info("MD5 signatures match")
            logging.info("Successful download")
            break
        else:
            logging.info("MD5 signatures didn't match")
            logging.info("Retrying download")
    return


def parse_xml(xml):
    """
    Parse an OData XML file to havest some relevant information re products
    available and so on. It will return a list of dictionaries, with one
    dictionary per product returned from the query. Each dicionary will have a
    number of keys (see ``fields_of_interest``), as well as ``link`` and
    ``qui
    """
    fields_of_interest = ["filename", "identifier", "instrumentshortname",
                          "orbitnumber", "orbitdirection", "producttype",
                          "beginposition", "endposition"]
    tree = ET.ElementTree(ET.fromstring(xml))
    # Search for all the acquired images...
    granules = []
    for elem in tree.iter(tag="{http://www.w3.org/2005/Atom}entry"):
        granule = {}
        for img in elem.getchildren():
            if img.tag.find("id") >= 0:
                granule['id'] = img.text
            if img.tag.find("link") and "href" in img.attrib:

                if img.attrib['href'].find("Quicklook") >= 0:
                    granule['quicklook'] = img.attrib['href']
                elif img.attrib['href'].find("$value") >= 0:
                    granule['link'] = img.attrib['href'].replace("$value", "")

            if "name" in img.attrib:
                if img.attrib['name'] in fields_of_interest:
                    granule[img.attrib['name']] = img.text

        granules.append(granule)

    return granules
    # print img.tag, img.attrib, img.text
    # for x in img.getchildren():


def download_sentinel(location, input_start_date, input_sensor, output_dir,
                      input_end_date=None, username="guest", password="guest",
                      cloud_pcntg=None, product_type=None):
    input_sensor = input_sensor.upper()
    sensor_list = ["S1", "S2"]
    if not input_sensor in sensor_list:
        raise ValueError("Sensor can only be S1 or S2. You provided %s"
                         % input_sensor)
    else:
        if input_sensor.upper() == "S1":
            sensor = "Sentinel-1"
        elif input_sensor.upper() == "S2":
            sensor = "Sentinel-2"
        sensor_str = 'platformname:%s' % sensor
        # sensor_str = 'filename:%s' % input_sensor.upper()
    try:
        start_date = datetime.datetime.strptime(input_start_date,
                                                "%Y.%m.%d").isoformat()
    except ValueError:
        try:
            start_date = datetime.datetime.strptime(input_start_date,
                                                    "%Y-%m-%d").isoformat()
        except ValueError:
            start_date = datetime.datetime.strptime(input_start_date,
                                                    "%Y/%j").isoformat()
    start_date = start_date + "Z"

    if input_end_date is None:
        end_date = "NOW"
    else:
        try:
            end_date = datetime.datetime.strptime(input_end_date,
                                                  "%Y.%m.%d").isoformat() + "Z"
        except ValueError:
            try:
                end_date = datetime.datetime.strptime(input_end_date,
                                                      "%Y-%m-%d").isoformat() + "Z"
            except ValueError:
                end_date = datetime.datetime.strptime(input_end_date,
                                                      "%Y/%j").isoformat() + "Z"

    if isinstance(location, str):
        location_str = f"*_{location}_*"
    elif len(location) == 2:
        location_str = 'footprint:"Intersects(%f, %f)"' % (location[0],
                                                           location[1])
    elif len(location) == 4:
        location_str = 'footprint:"Intersects( POLYGON((' + \
            '%f %f, %f %f, %f %f, %f %f, %f %f) ))"' % (
                location[0], location[1],
                location[0], location[3],
                location[2], location[3],
                location[2], location[1],
                location[0], location[1])
    time_str = f'beginposition:[{start_date:s} TO {end_date:s}]'
    query = f"{location_str:s} AND {time_str:s} AND {sensor_str:s}"
    if cloud_pcntg is not None:
            query = f"{query:s} AND cloudcoverpercentage:[0 TO {int(cloud_pcntg):d}]"
    if product_type is not None:
        if product_type is "L2A":
            query = f"{query:s} AND producttype:S2MSI2Ap"
        elif product_type is "L1C":
            query = f"{query:s} AND producttype:S2MSI1C"

    query = f"{hub_url}{query}"
    query = f"{query:s}&start=0&rows=100"

    # query = "%s%s" % ( hub_url, urllib2.quote(query ) )
    logging.debug(query)
    result = do_query(query, user=username, passwd=password)

    granules = parse_xml(result)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    ret_files = []
    for granule in granules:
        download_product(granule['link'] + "$value",
                         os.path.join(output_dir, granule['filename'].replace(
                             "SAFE", "zip")), user=username, passwd=password)
        ret_files.append(os.path.join(output_dir,
                                      granule['filename'].replace(
                                          "SAFE", "zip")))

    return granules, ret_files


def parse_aws_xml(xml_text, clouds=None):

    tree = ET.ElementTree(ET.fromstring(xml_text))
    root = tree.getroot()
    files_to_get = []
    for elem in tree.iter():
        for k in elem.getchildren():
            if k.tag.find("Key") >= 0:
                if k.text.find("tiles") >= 0:
                    files_to_get.append(k.text)

    if len(files_to_get) > 0 and clouds is not None:

        for fich in files_to_get:
            if fich.find("metadata.xml") >= 0:
                metadata_file = aws_url_dload + fich
                r = requests.get(metadata_file)
                tree = ET.ElementTree(ET.fromstring(r.text))
                root = tree.getroot()
                for cl in root.iter("CLOUDY_PIXEL_PERCENTAGE"):
                    if float(cl.text) > clouds:
                        return []
                    else:
                        return files_to_get
    return files_to_get


def aws_grabber(url, output_dir):
    output_fname = os.path.join(output_dir, url.split("tiles/")[-1])
    if not os.path.exists(os.path.dirname(output_fname)):
        # We should never get here, as the directory should always exist
        # Note that in parallel, this can sometimes create a race condition
        # Groan
        os.makedirs(os.path.dirname(output_fname))
    with open(output_fname, 'wb') as fp:
        while True:
            try:
                r = requests.get(url, stream=True)
                break
            except requests.execeptions.ConnectionError:
                time.sleep (240)
        for block in r.iter_content(8192):
            fp.write(block)
    logging.debug("Done with %s" % output_fname)
    return output_fname


def download_sentinel_amazon(start_date, output_dir,
                             tile=None,
                             longitude=None, latitude=None,
                             end_date=None, n_threads=15, just_previews=False,
                             verbose=False, clouds=None):
    """A method to download data from the Amazon cloud """
    # First, we get hold of the MGRS reference...
    if tile is None:
        mgrs_reference = get_mgrs(longitude, latitude)
    else:
        mgrs_reference = tile
    if verbose:
        logging.info(f"We need MGRS reference {mgrs_reference:s}")
    utm_code = mgrs_reference[:2]
    lat_band = mgrs_reference[2]
    square = mgrs_reference[3:]
    logging.info("Location coordinates: %s" % mgrs_reference)

    front_url = aws_url + "%s/%s/%s" % (utm_code, lat_band, square)
    this_date = start_date
    one_day = datetime.timedelta(days=1)
    files_to_download = []
    if end_date is None:
        end_date = datetime.datetime.today()
    logging.info("Scanning archive...")
    acqs_to_dload = 0
    while this_date <= end_date:

        the_url = "{0}{1}".format(front_url, "/{0:d}/{1:d}/{2:d}/0/".format(
            this_date.year, this_date.month, this_date.day))
        r = requests.get(the_url)
        more_files = parse_aws_xml(r.text, clouds=clouds)

        if len(more_files) > 0:
            acqs_to_dload += 1
            rqi = requests.get(the_url + "qi/")
            raux = requests.get(the_url + "aux/")
            qi = parse_aws_xml(rqi.text)
            aux = parse_aws_xml(raux.text)
            more_files.extend(qi)
            more_files.extend(aux)
            files_to_download.extend(more_files)
            LOG.info("Will download data for %s..." %
                     this_date.strftime("%Y/%m/%d"))

        this_date += one_day
    logging.info("Will download %d acquisitions" % acqs_to_dload)
    the_urls = []
    if just_previews:
        the_files = []
        for fich in files_to_download:
            if fich.find("preview") >= 0:
                the_files.append(fich)
        files_to_download = the_files

    for fich in files_to_download:
        the_urls.append(aws_url_dload + fich)
        ootput_dir = os.path.dirname(os.path.join(output_dir,
                                                  fich.split("tiles/")[-1]))
        if not os.path.exists(ootput_dir):

            LOG.info("Creating output directory (%s)" % ootput_dir)
            os.makedirs(ootput_dir)
    ok_files = []
    LOG.info("Downloading a grand total of %d files" %
             len(files_to_download))
    download_granule_patch = partial(aws_grabber, output_dir=output_dir)
    with futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        for fich in executor.map(download_granule_patch, the_urls):
            ok_files.append(fich)


if __name__ == "__main__":    # location = (43.3650, -8.4100)
    # input_start_date = "2015.01.01"
    # input_end_date = None

    # username = "guest"
    # password = "guest"

    # input_sensor = "S2"


    # output_dir = "/data/selene/ucfajlg/tmp/"
    # granules, retfiles = download_sentinel ( location, input_start_date,
    # input_sensor, output_dir )
    lng = -8.4100
    lat = 43.3650
    #lat = 39.0985 # Barrax
    #lng = -2.1082
    #lat = 28.55 # Libya 4
    #lng = 23.39
    #print("Testing S2 on AWS...")
    #lat=37.1972
    #lng=-4.0481
    #download_sentinel_amazon(datetime.datetime(2016, 1, 11), "/tmp/",
    #                         end_date=datetime.datetime(2016, 12, 25),
    #                         longitude=lng, latitude=lat,
    #                         clouds=10)
    #break
    #print("Testing S2 on COPERNICUS scientific hub")
    location=(lat,lng)
    input_start_date="2017.1.11"
    input_sensor="S2"
    output_dir="/home/ucfajlg/temp/stuff/"
    #print("Set username and password variables for Sentinel hub!!!")
    #location="T50SLG"
    username = "jgomezdans"
    password = "2CKwSjva"
    download_sentinel(location, input_start_date, input_sensor, output_dir,
                      input_end_date=None, username=username,
                      password=password, cloud_pcntg=20, product_type="L2A")
