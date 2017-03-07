#!/usr/bin/env python

"""
This extractor triggers when a file is added to a dataset in Clowder.

It checks for _left and _right BIN files to convert them into
JPG and TIF formats.
 """

import os
import logging
import tempfile
import shutil

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets

import bin_to_geotiff as bin2tiff


def determineOutputDirectory(outputRoot, dsname):
    if dsname.find(" - ") > -1:
        timestamp = dsname.split(" - ")[1]
    else:
        timestamp = "dsname"
    if timestamp.find("__") > -1:
        datestamp = timestamp.split("__")[0]
    else:
        datestamp = ""

    return os.path.join(outputRoot, datestamp, timestamp)

def addFileIfNecessaryaddFileIfNecessary(connector, host, secret_key, resource, filepath):
    # Upload file to a dataset, unless it already exists
    for f in resource['files']:
        if 'filename' in f:
            if f['filepath'] == filepath:
                return False

    print("Uploading existing file to dataset: %s" % filepath)
    pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], filepath)

    return True

class StereoBin2JpgTiff(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')
        self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
                                 default="/home/extractor/sites/ua-mac/Level_1/demosaic",
                                 help="root directory where timestamp & output directories will be created")
        self.parser.add_argument('--overwrite', dest="force_overwrite", type=bool, nargs='?', default=False,
                                 help="whether to overwrite output file if it already exists in output directory")

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        # assign other arguments
        self.output_dir = self.args.output_dir
        self.force_overwrite = self.args.force_overwrite

    def check_message(self, connector, host, secret_key, resource, parameters):
        img_left, img_right, metadata = None, None, None

        # If there is no _left and _right .bin, ignore
        for f in resource['files']:
            if 'filename' in f:
                if f['filename'].endswith('_left.bin'):
                    img_left = f['filename']
                elif f['filename'].endswith('_right.bin'):
                    img_right = f['filename']
        if not (img_left and img_right):
            logging.info("skipping %s; left & right not found" % resource['id'])
            return CheckMessage.ignore

        # Check if outputs already exist, unless overwrite is specified
        if not self.force_overwrite:
            out_dir = determineOutputDirectory(self.output_dir, resource['dataset_info']['name'])
            lbase = os.path.basename(img_left)[:-4]
            rbase = os.path.basename(img_right)[:-4]
            left_jpg = os.path.join(out_dir, lbase+'.jpg')
            right_jpg = os.path.join(out_dir, rbase+'.jpg')
            left_tiff = os.path.join(out_dir, lbase+'.tif')
            right_tiff = os.path.join(out_dir, rbase+'.tif')

            # If they exist, check if outputs are already in the dataset, and add them if not
            if (os.path.isfile(left_jpg) and os.path.isfile(right_jpg) and
                    os.path.isfile(left_tiff) and os.path.isfile(right_tiff)):
                addFileIfNecessaryaddFileIfNecessary(connector, host, secret_key, resource, left_jpg)
                addFileIfNecessaryaddFileIfNecessary(connector, host, secret_key, resource, right_jpg)
                addFileIfNecessaryaddFileIfNecessary(connector, host, secret_key, resource, left_tiff)
                addFileIfNecessaryaddFileIfNecessary(connector, host, secret_key, resource, right_tiff)

                logging.info("skipping %s, outputs already exist" % resource['id'])
                return CheckMessage.ignore

        # Check if we have the necessary metadata for the dataset also
        meta_json = pyclowder.datasets.download_metadata(connector, host, secret_key, resource['id'])
        for md in meta_json:
            if 'lemnatec_measurement_metadata' in md['content']:
                metadata = True

        if img_left and img_right and metadata:
            return CheckMessage.download
        else:
            logging.info("skipping %s; metadata not found" % resource['id'])
            return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        metafile, img_left, img_right, metadata = None, None, None, None

        # Get left/right files and metadata
        for fname in resource['local_paths']:
            # First check metadata attached to dataset in Clowder for item of interest
            if fname.endswith('_dataset_metadata.json'):
                all_dsmd = bin2tiff.load_json(fname)
                for curr_dsmd in all_dsmd:
                    if 'content' in curr_dsmd and 'lemnatec_measurement_metadata' in curr_dsmd['content']:
                        metafile = fname
                        metadata = curr_dsmd['content']
            # Otherwise, check if metadata was uploaded as a .json file
            elif fname.endswith('_metadata.json') and fname.find('/_metadata.json') == -1 and metafile is None:
                metafile = fname
                metadata = bin2tiff.load_json(metafile)
            elif fname.endswith('_left.bin'):
                img_left = fname
            elif fname.endswith('_right.bin'):
                img_right = fname
        if None in [metafile, img_left, img_right, metadata]:
            logging.error('could not find all 3 of left/right/metadata')
            return

        out_dir = determineOutputDirectory(self.output_dir, resource['dataset_info']['name'])
        logging.info("...writing outputs to: %s" % out_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        metadata = bin2tiff.lower_keys(metadata)
        # Determine output files
        lbase = os.path.basename(img_left)[:-4]
        rbase = os.path.basename(img_right)[:-4]
        left_jpg = os.path.join(out_dir, lbase+'.jpg')
        right_jpg = os.path.join(out_dir, rbase+'.jpg')
        left_tiff = os.path.join(out_dir, lbase+'.tif')
        right_tiff = os.path.join(out_dir, rbase+'.tif')

        logging.info("...determining image shapes")
        left_shape = bin2tiff.get_image_shape(metadata, 'left')
        right_shape = bin2tiff.get_image_shape(metadata, 'right')

        center_position = bin2tiff.get_position(metadata) # (x, y, z) in meters
        fov = bin2tiff.get_fov(metadata, center_position[2], left_shape) # (fov_x, fov_y) in meters; need to pass in the camera height to get correct fov
        left_position = [center_position[0]+bin2tiff.STEREO_OFFSET, center_position[1], center_position[2]]
        right_position = [center_position[0]-bin2tiff.STEREO_OFFSET, center_position[1], center_position[2]]
        left_gps_bounds = bin2tiff.get_bounding_box(left_position, fov) # (lat_max, lat_min, lng_max, lng_min) in decimal degrees
        right_gps_bounds = bin2tiff.get_bounding_box(right_position, fov)

        logging.info("...creating JPG images")
        left_image = bin2tiff.process_image(left_shape, img_left, left_jpg)
        right_image = bin2tiff.process_image(right_shape, img_right, right_jpg)
        logging.info("...uploading output JPGs to dataset")
        pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], left_jpg)
        pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'],right_jpg)

        logging.info("...creating geoTIFF images")
        # Rename out.tif after creation to avoid long path errors
        out_tmp_tiff = tempfile.mkstemp()
        bin2tiff.create_geotiff('left', left_image, left_gps_bounds, out_tmp_tiff[1])
        shutil.copyfile(out_tmp_tiff[1], left_tiff)
        os.remove(out_tmp_tiff[1])
        out_tmp_tiff = tempfile.mkstemp()
        bin2tiff.create_geotiff('right', right_image, right_gps_bounds, out_tmp_tiff[1])
        shutil.copyfile(out_tmp_tiff[1], left_tiff)
        shutil.copyfile(out_tmp_tiff[1], right_tiff)
        os.remove(out_tmp_tiff[1])
        logging.info("...uploading output geoTIFFs to dataset")
        pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'], left_tiff)
        pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['id'],right_tiff)

if __name__ == "__main__":
    extractor = StereoBin2JpgTiff()
    extractor.start()
