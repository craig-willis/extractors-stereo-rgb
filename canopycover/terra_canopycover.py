#!/usr/bin/env python

import json
import logging
import re
from numpy import asarray, rollaxis

from pyclowder.utils import CheckMessage
from pyclowder.datasets import download_metadata, get_info, upload_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy
from terrautils.betydb import add_arguments, get_sites, get_sites_by_latlon, submit_traits, \
    get_site_boundaries
from terrautils.geostreams import create_datapoint_with_dependencies
from terrautils.gdal import clip_raster, centroid_from_geojson
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata

import canopyCover as ccCore


logging.basicConfig(format='%(asctime)s %(message)s')

def add_local_arguments(parser):
    # add any additional arguments to parser
    add_arguments(parser)

class CanopyCoverHeight(TerrarefExtractor):
    def __init__(self):
        super(CanopyCoverHeight, self).__init__()

        add_local_arguments(self.parser)

        # parse command line and load default logging configuration
        self.setup(sensor='stereoTop_canopyCover')

        # assign other argumentse
        self.bety_url = self.args.bety_url
        self.bety_key = self.args.bety_key

    def check_message(self, connector, host, secret_key, resource, parameters):
        if resource['name'].find('fullfield') > -1 and re.match("^.*\d+_rgb_.*thumb.tif", resource['name']):
            # Check metadata to verify we have what we need
            md = download_metadata(connector, host, secret_key, resource['parent']['id'])
            if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
                logging.info("skipping dataset %s; metadata indicates it was already processed" % resource['id'])
                return CheckMessage.ignore
            return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        tmp_csv = "canopycovertraits.csv"
        csv_file = open(tmp_csv, 'w')
        (fields, traits) = ccCore.get_traits_table()
        csv_file.write(','.join(map(str, fields)) + '\n')

        # Get full list of experiment plots using date as filter
        logging.info(connector)
        logging.info(host)
        logging.info(secret_key)
        logging.info(resource)
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]
        all_plots = get_site_boundaries(timestamp, city='Maricopa')

        successful_plots = 0
        for plotname in all_plots:
            bounds = all_plots[plotname]

            # Use GeoJSON string to clip full field to this plot
            try:
                (pxarray, geotrans) = clip_raster(resource['local_paths'][0], bounds)
                if len(pxarray.shape) < 3:
                    logging.error("unexpected array shape for %s (%s)" % (plotname, pxarray.shape))
                    continue
                ccVal = ccCore.gen_cc_for_img(rollaxis(pxarray,0,3), 5)
                ccVal *= 100.0 # Make 0-100 instead of 0-1
                successful_plots += 1
                if successful_plots % 10 == 0:
                    logging.info("processed %s/%s plots successfully" % (successful_plots, len(all_plots)))
            except:
                logging.error("error generating cc for %s" % plotname)
                continue

            traits['canopy_cover'] = str(ccVal)
            traits['site'] = plotname
            traits['local_datetime'] = timestamp+"T12:00:00"
            trait_list = ccCore.generate_traits_list(traits)

            csv_file.write(','.join(map(str, trait_list)) + '\n')

            # Prepare and submit datapoint
            centroid_lonlat = json.loads(centroid_from_geojson(bounds))["coordinates"]
            time_fmt = timestamp+"T12:00:00-07:00"
            dpmetadata = {
                "source": host + ("" if host.endswith("/") else "/") + "files/" + resource['id'],
                "canopy_cover": ccVal
            }
            create_datapoint_with_dependencies(connector, host, secret_key, "Canopy Cover",
                                               (centroid_lonlat[1], centroid_lonlat[0]), time_fmt, time_fmt,
                                               dpmetadata, timestamp)

        # submit CSV to BETY
        csv_file.close()
        submit_traits(tmp_csv, betykey=self.bety_key)

        # Add metadata to original dataset indicating this was run
        ext_meta = build_metadata(host, self.extractor_info, resource['parent']['id'], {
            "plots_processed": successful_plots,
            "plots_skipped": len(all_plots)-successful_plots,
            "betydb_link": "https://terraref.ncsa.illinois.edu/bety/api/beta/variables?name=canopy_cover"
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['parent']['id'], ext_meta)

        self.end_message()

if __name__ == "__main__":
    extractor = CanopyCoverHeight()
    extractor.start()
