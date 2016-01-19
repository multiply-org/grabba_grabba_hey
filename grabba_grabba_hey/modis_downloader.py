#!/usr/bin/env python
"""
Refactoring the MODIS downloading tool. The code is now simpler and
has been implemented to have concurrent downloads. The cost of this is the
addition of the ``concurrent`` and ``requests`` packages as dependencies.
"""
from functools import partial
import os
import datetime
import time

import requests
from concurrent import futures


BASE_URL = "http://e4ftl01.cr.usgs.gov/"

class WebError (RuntimeError):
    """An exception for web issues"""
    def __init__(self, arg):
        self.args = arg


def get_available_dates(url, start_date, end_date=None):
    """
    This function gets the available dates for a particular
    product, and returns the ones that fall within a particular
    pair of dates. If the end date is set to ``None``, it will
    be assumed it is today.
    """
    if end_date is None:
        end_date = datetime.datetime.now()
    r = requests.get(url)
    if not r.ok:
        raise WebError(
            "Problem contacting NASA server. Either server " +
            "is down, or the product you used (%s) is kanckered" %
            url)
    html = r.text
    avail_dates = []
    for line in html.splitlines()[19:]:
        if line.find("[DIR]") >= 0 and line.find("href") >= 0:
            this_date = line.split("href=")[1].split('"')[1].strip("/")
            this_datetime = datetime.datetime.strptime(this_date,
                                                       "%Y.%m.%d")
            if this_datetime >= start_date and this_datetime <= end_date:
                avail_dates.append(url + "/" + this_date)
    return avail_dates


def download_granule_list(url, tiles):
    """For a particular product and date, obtain the data granule URLs.

    """
    if not isinstance(tiles, type([])):
        tiles = [tiles]
    while True:
        try:
            r = requests.get(url )
            break
        except requests.execeptions.ConnectionError:
            sleep ( 240 )
            
    grab = []
    for line in r.text.splitlines():
        for tile in tiles:
            if line.find ( tile ) >= 0 and line.find (".xml" ) < 0 \
                    and line.find("BROWSE") < 0:
                fname = line.split("href=")[1].split('"')[1]
                grab.append(url + "/" + fname)
    return grab


def download_granules(url, output_dir):
    fname = url.split("/")[-1]
    output_fname = os.path.join(output_dir, fname)
    with open(output_fname, 'wb') as fp:
        while True:
            try:
                r = requests.get(url, stream=True)
                break
            except requests.execeptions.ConnectionError:
                sleep ( 240 )
        for block in r.iter_content(8192):
            fp.write(block)
    print "Done with %s" % output_fname
    return output_fname


def get_modis_data(platform, product, tiles, output_dir, start_date,
                   end_date=None, n_threads=5):
    """The main workhorse of MODIS downloading. This function will grab
    products for a particular platform (MOLT, MOLA or MOTA). The products
    are specified by their MODIS code (e.g. MCD45A1.051 or MOD09GA.006).
    You need to specify a tile (or a list of tiles), as well as a starting
    and end date. If the end date is not specified, the current date will
    be chosen. Additionally, you can specify the number of parallel threads
    to use. And you also need to give an output directory to dump your files.

    Parameters
    -----------
    platform: str
        The platform, MOLT, MOLA or MOTA. This basically relates to the sensor
        used (or if a combination of AQUA & TERRA is used)
    product: str
        The MODIS product. The product name should be in MODIS format
        (MOD09Q1.006, so product acronym dot collection)
    tiles: str or iter
        A string with a single tile (e.g. "h17v04") or a lits of such strings.
    output_dir: str
        The output directory
    start_date: datetime
        The starting date as a datetime object
    end_date: datetime
        The end date as a datetime object. If not specified, taken as today.
    n_threads: int
        The number of concurrent downloads to envisage. I haven't got a clue
        as to what a good number would be here...

    """
    # Ensure the platform is OK
    assert platform.upper() in [ "MOLA", "MOLT", "MOTA"], \
        "%s is not a valid platform. Valid ones are MOLA, MOLT, MOTA" % \
        platform
    # If output directory doesn't exist, create it
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    # Cook the URL for the product
    url = BASE_URL + platform + "/" + product
    # Get all the available dates in the NASA archive...
    the_dates = get_available_dates(url, start_date, end_date=end_date)
    # We then explore the NASA archive for the dates that we are going to
    # download. This is done in parallel. For each date, we will get the
    # url for each of the tiles that are required.
    the_granules = []
    download_granule_patch = partial(download_granule_list, tiles=tiles)
    with futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        for granules in executor.map(download_granule_patch, the_dates):
            the_granules.append(granules)
    # Flatten the list of lists...
    gr = [g for granule in the_granules for g in granule]
    gr.sort()
    print "Will download %d files" % len ( gr )
    download_granule_patch = partial(download_granules,
                                     output_dir=output_dir)
    # Wait for a few minutes before downloading the data
    sleep ( 240 )
    # The main download loop. This will get all the URLs with the filenames,
    # and start downloading them in parallel.
    dload_files = []
    with futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        for fich in executor.map(download_granule_patch, gr):
            dload_files.append(fich)
    return dload_files
