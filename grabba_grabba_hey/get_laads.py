#!/usr/bin/env python

import sys
import logging
import os
import json
from functools import partial

import requests
from concurrent import futures

logging.basicConfig(level=logging.INFO)

LOG = logging.getLogger(__name__)

def download_granule(url, output_directory="."):
    r = requests.get( url, stream=True )
    if not r.ok:
        raise IOError("Can't start download of %s" % url)
    
    fname = url.split("/")[-1]
    LOG.debug("Getting %s from %s" % (fname, url))
    file_size = int(r.headers['content-length'])
    LOG.debug("\t%s file size: %d" % (fname, file_size))
    output_fname = os.path.join(output_dir, fname)
    with open(output_fname, 'wb') as fp:
        for block in r.iter_content(65536):
            fp.write(block)
    LOG.info("Done with %s" % output_fname)
    return output_fname
    

def get_laads_files(laad_query_file, output_dir, n_threads=10):
    parent_url = "https://ladsweb.modaps.eosdis.nasa.gov/"
    jj=json.load(open(laad_query_file, 'r'))
    urls = []
    for granule in jj.iterkeys():
        if granule != "query":
            the_url = "%s/%s" % (parent_url, jj[granule]["url"])
            urls.append(the_url)
    dloaded_files = []
    download_granule_patch = partial(download_granule, 
                                     output_directory=output_dir)
    with futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        for fich in executor.map(download_granule_patch, urls):
            dloaded_files.append(fich)
    LOG.info("Done downloading!")

if __name__ == "__main__":
    query_file = sys.argv[1]
    output_dir = sys.argv[2]
    get_laads_files(query_file, output_dir)
