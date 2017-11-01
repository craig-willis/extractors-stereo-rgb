#!/usr/bin/env python

"""
This extractor triggers when a file is added to a dataset in Clowder.

It checks for _left and _right BIN files to convert them into
JPG and TIF formats.
 """

import os
import logging
import shutil
import time
import datetime

from pyclowder.utils import CheckMessage
from pyclowder.datasets import download_metadata, upload_metadata, remove_metadata
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy, upload_to_dataset
from terrautils.formats import create_geotiff, create_image
from terrautils.spatial import geojson_to_tuples

import bin_to_geotiff as bin2tiff


#logging.basicConfig(format='%(asctime)s %(message)s')

class StereoBin2JpgTiff(TerrarefExtractor):
    def __init__(self):
        super(StereoBin2JpgTiff, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='rgb_geotiff')

    def check_message(self, connector, host, secret_key, resource, parameters):
        now = time.time()
        logging.getLogger(__name__).info("------- Begin check message")

        if not is_latest_file(resource):
            return CheckMessage.ignore

        elapsed = time.time() - now
        now = time.time()
        logging.getLogger(__name__).info("%s checked latest" % elapsed)

        # Check for a left and right BIN file - skip if not found
        found_left = False
        found_right = False
        for f in resource['files']:
            if 'filename' in f:
                if f['filename'].endswith('_left.bin'):
                    found_left = True
                elif f['filename'].endswith('_right.bin'):
                    found_right = True
        if not (found_left and found_right):
            return CheckMessage.ignore

        # Check if outputs already exist unless overwrite is forced - skip if found
        if not self.overwrite:
            timestamp = resource['dataset_info']['name'].split(" - ")[1]
            lbase = self.sensors.get_sensor_path(timestamp, opts=['left'], ext='')
            rbase = self.sensors.get_sensor_path(timestamp, opts=['right'], ext='')
            out_dir = os.path.dirname(lbase)
            if (os.path.isfile(lbase+'jpg') and os.path.isfile(rbase+'jpg') and
                    os.path.isfile(lbase+'tif') and os.path.isfile(rbase+'tif')):
                logging.info("skipping dataset %s; outputs found in %s" % (resource['id'], out_dir))
                return CheckMessage.ignore

        elapsed = time.time() - now
        now = time.time()
        logging.getLogger(__name__).info("%s Got necessary details" % elapsed)

        # Check metadata to verify we have what we need
        md = download_metadata(connector, host, secret_key, resource['id'])

        elapsed = time.time() - now
        now = time.time()
        logging.getLogger(__name__).info("%s Downloaded md" % elapsed)
        if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
            logging.info("skipping dataset %s; metadata indicates it was already processed" % resource['id'])
            return CheckMessage.ignore
        if get_terraref_metadata(md) and found_left and found_right:
            elapsed = time.time() - now
            now = time.time()
            logging.getLogger(__name__).info("%s Approved for processing" % elapsed)
            return CheckMessage.download
        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()
        now = time.time()

        logging.getLogger(__name__).info("------- begin process message")

        # Get left/right files and metadata
        img_left, img_right, metadata = None, None, None
        for fname in resource['local_paths']:
            if fname.endswith('_dataset_metadata.json'):
                all_dsmd = load_json_file(fname)
                metadata = get_terraref_metadata(all_dsmd, 'stereoTop')
            elif fname.endswith('_left.bin'):
                img_left = fname
            elif fname.endswith('_right.bin'):
                img_right = fname
        if None in [img_left, img_right, metadata]:
            raise ValueError("could not locate each of left+right+metadata in processing")

        # Determine output location & filenames
        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        left_tiff = self.sensors.create_sensor_path(timestamp, opts=['left'])
        right_tiff = self.sensors.create_sensor_path(timestamp, opts=['right'])
        # left_jpg = left_tiff.replace('.tif', '.jpg')
        # right_jpg = right_tiff.replace('.tif', '.jpg')
        uploaded_file_ids = []

        elapsed = time.time() - now
        now = time.time()
        logging.getLogger(__name__).info("%s got required inputs" % elapsed)

        logging.info("...determining image shapes")
        left_shape = bin2tiff.get_image_shape(metadata, 'left')
        right_shape = bin2tiff.get_image_shape(metadata, 'right')
        left_gps_bounds = geojson_to_tuples(metadata['spatial_metadata']['left']['bounding_box'])
        right_gps_bounds = geojson_to_tuples(metadata['spatial_metadata']['right']['bounding_box'])
        out_tmp_tiff = "/home/extractor/"+resource['dataset_info']['name']+".tif"

        elapsed = time.time() - now
        now = time.time()
        logging.getLogger(__name__).info("%s got image shapes" % elapsed)

        target_dsid = build_dataset_hierarchy(host, secret_key, self.clowder_user, self.clowder_pass, self.clowderspace,
                                              self.sensors.get_display_name(),
                                              timestamp[:4], timestamp[5:7], timestamp[8:10],
                                              leaf_ds_name=self.sensors.get_display_name()+' - '+timestamp)

        elapsed = time.time() - now
        now = time.time()
        logging.getLogger(__name__).info("%s created hierarchy" % elapsed)

        if (not os.path.isfile(left_tiff)) or self.overwrite:
            logging.info("...creating & uploading left geoTIFF")
            left_image = bin2tiff.process_image(left_shape, img_left, None)
            # Rename output.tif after creation to avoid long path errors
            create_geotiff(left_image, left_gps_bounds, out_tmp_tiff, None, False, self.extractor_info, metadata)

            elapsed = time.time() - now
            now = time.time()
            logging.getLogger(__name__).info("%s created left" % elapsed)

            shutil.move(out_tmp_tiff, left_tiff)
            if left_tiff not in resource['local_paths']:
                fileid = upload_to_dataset(connector, host, self.clowder_user, self.clowder_pass, target_dsid, left_tiff)
                uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)

                elapsed = time.time() - now
                now = time.time()
                logging.getLogger(__name__).info("%s uploaded left" % elapsed)
            self.created += 1
            self.bytes += os.path.getsize(left_tiff)

        if (not os.path.isfile(right_tiff)) or self.overwrite:
            logging.info("...creating & uploading right geoTIFF")
            right_image = bin2tiff.process_image(right_shape, img_right, None)
            create_geotiff(right_image, right_gps_bounds, out_tmp_tiff, None, False, self.extractor_info, metadata)

            elapsed = time.time() - now
            now = time.time()
            logging.getLogger(__name__).info("%s created right" % elapsed)

            shutil.move(out_tmp_tiff, right_tiff)
            if right_tiff not in resource['local_paths']:
                fileid = upload_to_dataset(connector, host, self.clowder_user, self.clowder_pass, target_dsid,right_tiff)
                uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)

                elapsed = time.time() - now
                now = time.time()
                logging.getLogger(__name__).info("%s uploaded right" % elapsed)
            self.created += 1
            self.bytes += os.path.getsize(right_tiff)



        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        ext_meta = build_metadata(host, self.extractor_info, resource['id'], {
                "files_created": uploaded_file_ids
            }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['id'], ext_meta)

        elapsed = time.time() - now
        now = time.time()
        logging.getLogger(__name__).info("%s uploaded raw md" % elapsed)

        # Upload original Lemnatec metadata to new Level_1 dataset
        md = get_terraref_metadata(all_dsmd)
        md['raw_data_source'] = host + ("" if host.endswith("/") else "/") + "datasets/" + resource['id']
        lemna_md = build_metadata(host, self.extractor_info, target_dsid, md, 'dataset')
        upload_metadata(connector, host, secret_key, target_dsid, lemna_md)

        elapsed = time.time() - now
        now = time.time()
        logging.getLogger(__name__).info("%s uploaded lemna md" % elapsed)

        logging.getLogger(__name__).info("--------------- START=%s      END=%s" %
                                         (self.starttime, datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')))

        self.end_message()

if __name__ == "__main__":
    extractor = StereoBin2JpgTiff()
    extractor.start()
