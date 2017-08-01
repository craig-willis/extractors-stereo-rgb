#!/usr/bin/env python

import os
import logging
import requests
import subprocess

import datetime
from dateutil.parser import parse

from terrautils.extractors import TerrarefExtractor
import terrautils.extractors

from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets

import full_day_to_tiles
import shadeRemoval as shade


def add_local_arguments(parser):

    self.parser.add_argument('--darker', type=bool, default=False,
            help="whether to use multipass mosiacking to select darker pixels")

    self.parser.add_argument('--split', type=int, default=2,
            help="number of splits to use if --darker is True")


class FullFieldMosaicStitcher(TerrarefExtractor):

    def __init__(self):

        super(FullFieldMosaicStitcher, self).__init__()

        add_local_arguments(self.parser)

        # parse command line and initialize extractor
        self.setup()

        # process local arguments
        self.generate_darker = self.args.darker
        self.split = self.args.split


    def check_message(self, connector, host, secret_key, 
                      resource, parameters):
        return CheckMessage.bypass


    def process_message(self, connector, host, secret_key, 
                        resource, parameters):

        self.start_message()
        created = 0
        bytes = 0

        # get full path to sensor primary result file
        out_tif_full = self.get_sensor_path(timestamp)
        out_dir = os.path.dirname(out_tif_full)
        out_vrt = self.get_sensor_path(timestamp, ext='.vrt')
        out_vrt = self.get_sensor_path(timestamp, ext='_thumb.tif')

        nu_created, nu_bytes = 0, 0
        if not self.generate_darker:
            (nu_created, nu_bytes) = self.generateSingleMosaic(out_dir, out_vrt, out_tif_thumb, out_tif_full, parameters)
        else:
            (nu_created, nu_bytes) = self.generateDarkerMosaic(out_dir, out_vrt, out_tif_thumb, out_tif_full, parameters)
        created += nu_created
        bytes += nu_bytes

        # Upload full field image to Clowder
        parent_collect = self.getCollectionOrCreate(connector, host, secret_key, "Full Field Stitched Mosaics",
                                                    parent_space=self.mainspace)
        year_collect = self.getCollectionOrCreate(connector, host, secret_key, parameters["output_dataset"][:17],
                                                  parent_collect, self.mainspace)
        month_collect = self.getCollectionOrCreate(connector, host, secret_key, parameters["output_dataset"][:20],
                                                   year_collect, self.mainspace)
        target_dsid = self.getDatasetOrCreate(connector, host, secret_key, parameters["output_dataset"],
                                              month_collect, self.mainspace)

        thumbid = pyclowder.files.upload_to_dataset(connector, host, secret_key, target_dsid, out_tif_thumb)
        fullid = pyclowder.files.upload_to_dataset(connector, host, secret_key, target_dsid, out_tif_full)

        content = {
            "comment": "This stitched image is computed based on an assumption that the scene is planar. \
                There are likely to be be small offsets near the boundary of two images anytime there are plants \
                at the boundary (because those plants are higher than the ground plane), or where the dirt is \
                slightly higher or lower than average.",
            "file_ids": parameters["file_ids"]
        }
        thumbmeta = terrautils.extractors.build_metadata(host, self.extractor_info['name'], thumbid, content, 'file')
        pyclowder.files.upload_metadata(connector, host, secret_key, thumbid, thumbmeta)
        fullmeta = terrautils.extractors.build_metadata(host, self.extractor_info['name'], fullid, content, 'file')
        pyclowder.files.upload_metadata(connector, host, secret_key, thumbid, fullmeta)

        self.end_message(created, bytes)


    def getCollectionOrCreate(self, connector, host, secret_key, cname, parent_colln=None, parent_space=None):
        # Fetch dataset from Clowder by name, or create it if not found
        url = "%sapi/collections?key=%s&title=" % (host, secret_key, cname)
        result = requests.get(url, verify=connector.ssl_verify)
        result.raise_for_status()

        if len(result.json()) == 0:
            return pyclowder.collections.create_empty(connector, host, secret_key, cname, "",
                                                      parent_colln, parent_space)
        else:
            return result.json()[0]['id']

    def getDatasetOrCreate(self, connector, host, secret_key, dsname, parent_colln=None, parent_space=None):
        # Fetch dataset from Clowder by name, or create it if not found
        url = "%sapi/datasets?key=%s&title=" % (host, secret_key, dsname)
        result = requests.get(url, verify=connector.ssl_verify)
        result.raise_for_status()

        if len(result.json()) == 0:
            return pyclowder.datasets.create_empty(connector, host, secret_key, dsname, "",
                                                   parent_colln, parent_space)
        else:
            return result.json()[0]['id']

    def generateSingleMosaic(self, out_dir, out_vrt, out_tif_thumb, out_tif_full, parameters):
        # Create simple mosaic from geotiff list
        created, bytes = 0, 0

        if (not os.path.isfile(out_vrt)) or self.overwrite:
            logging.info("processing %s TIFs" % len(parameters['file_ids']))

            # Write input list to tmp file
            with open("tiflist.txt", "w") as tifftxt:
                for t in parameters["file_ids"]:
                    tifftxt.write("%s/n" % t)

            # Create VRT from every GeoTIFF
            logging.info("Creating %s..." % out_vrt)
            full_day_to_tiles.createVrtPermanent(out_dir, "tiflist.txt", out_vrt)
            os.remove("tiflist.txt")
            created += 1
            bytes += os.path.getsize(out_vrt)

        if (not os.path.isfile(out_tif_thumb)) or self.overwrite:
            # Convert VRT to full-field GeoTIFF (low-res then high-res)
            logging.info("Converting VRT to %s..." % out_tif_thumb)
            subprocess.call(["gdal_translate -projwin -111.9750277 33.0764277 -111.9748097 33.0745861 "+
                             "-outsize 10% 10% %s %s" % (out_vrt, out_tif_thumb)])
            created += 1
            bytes += os.path.getsize(out_tif_thumb)

        if (not os.path.isfile(out_tif_full)) or self.overwrite:
            logging.info("Converting VRT to %s..." % out_tif_full)
            subprocess.call(["gdal_translate -projwin -111.9750277 33.0764277 -111.9748097 33.0745861 "+
                             "%s %s" % (out_vrt, out_tif_full)])
            created += 1
            bytes += os.path.getsize(out_tif_full)

        return (created, bytes)

    def generateDarkerMosaic(self, out_dir, out_vrt, out_tif_thumb, out_tif_full, parameters):
        # Create dark-pixel mosaic from geotiff list using multipass for darker pixel selection
        created, bytes = 0, 0

        if (not os.path.isfile(out_vrt)) or self.overwrite:
            # Write input list to tmp file
            with open("tiflist.txt", "w") as tifftxt:
                for t in parameters["file_ids"]:
                    tifftxt.write("%s/n" % t)

            # Split full tiflist into parts according to split number
            shade.split_tif_list("tiflist.txt", out_dir, self.split_num)
            os.remove("tiflist.txt")

            # Generate tiles from each split VRT into numbered folders
            shade.create_diff_tiles_set(out_dir, self.split_num)

            # Choose darkest pixel from each overlapping tile
            unite_tiles_dir = os.path.join(out_dir, 'unite')
            shade.integrate_tiles(out_dir, unite_tiles_dir, self.split_num)

            # If any files didn't have overlap, copy individual tile
            shade.copy_missing_tiles(out_dir, unite_tiles_dir, self.split_num, tiles_folder_name='tiles_left')

            # Create output VRT from overlapped tiles
            # TODO: Adjust this step so google HTML isn't generated?
            shade.create_unite_tiles(unite_tiles_dir, out_vrt)
            created += 1
            bytes += os.path.getsize(out_vrt)

        if (not os.path.isfile(out_tif_thumb)) or self.overwrite:
            # Convert VRT to full-field GeoTIFF (low-res then high-res)
            logging.info("Converting VRT to %s..." % out_tif_thumb)
            subprocess.call(["gdal_translate -projwin -111.9750277 33.0764277 -111.9748097 33.0745861 "+
                             "-outsize 10% 10% %s %s" % (out_vrt, out_tif_thumb)])
            created += 1
            bytes += os.path.getsize(out_tif_thumb)

        if (not os.path.isfile(out_tif_full)) or self.overwrite:
            logging.info("Converting VRT to %s..." % out_tif_full)
            subprocess.call(["gdal_translate -projwin -111.9750277 33.0764277 -111.9748097 33.0745861 "+
                             "%s %s" % (out_vrt, out_tif_full)])
            created += 1
            bytes += os.path.getsize(out_tif_full)

        return (created, bytes)


if __name__ == "__main__":
    extractor = FullFieldMosaicStitcher()
    extractor.start()
