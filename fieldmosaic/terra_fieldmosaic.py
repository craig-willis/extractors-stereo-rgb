#!/usr/bin/env python

import os
import logging
import subprocess
import json

from pyclowder.utils import CheckMessage
from pyclowder.files import upload_metadata, download_info
from terrautils.extractors import TerrarefExtractor, build_metadata, build_dataset_hierarchy, \
    upload_to_dataset, create_empty_collection, create_empty_dataset

import full_day_to_tiles
import shadeRemoval as shade




def add_local_arguments(parser):
    # add any additional arguments to parser
    parser.add_argument('--darker', type=bool, default=os.getenv('MOSAIC_DARKER', False),
                             help="whether to use multipass mosiacking to select darker pixels")
    parser.add_argument('--split', type=int, default=os.getenv('MOSAIC_SPLIT', 2),
                             help="number of splits to use if --darker is True")

class FullFieldMosaicStitcher(TerrarefExtractor):
    def __init__(self):
        super(FullFieldMosaicStitcher, self).__init__()

        add_local_arguments(self.parser)

        # parse command line and load default logging configuration
        self.setup(sensor='fullfield')

        # assign local arguments
        self.darker = self.args.darker
        self.split = self.args.split

    def check_message(self, connector, host, secret_key, resource, parameters):
        return CheckMessage.bypass

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        if type(parameters) is str:
            parameters = json.loads(parameters)
        if 'parameters' in parameters:
            parameters = parameters['parameters']
        if type(parameters) is unicode:
            parameters = json.loads(str(parameters))

        # Input path will suggest which sensor we are seeing
        sensor_type = None
        for f in resource['files']:
            filepath = f['filepath']
            for sens in ["rgb_geotiff", "ir_geotiff", "laser3d_heightmap"]:
                if filepath.find(sens) > -1:
                    sensor_type = sens.split("_")[0]
                    break
            if sensor_type is not None:
                break

        # output_dataset = "Full Field - 2017-01-01"
        dataset_name = parameters["output_dataset"]
        timestamp = dataset_name.split(" - ")[1]

        if 'scan_type' in parameters:
            out_tif_full = self.sensors.create_sensor_path(timestamp, opts=[sensor_type, parameters["scan_type"]])
        else:
            out_tif_full = self.sensors.create_sensor_path(timestamp, opts=[sensor_type])
        out_tif_thumb = out_tif_full.replace(".tif", "_thumb.tif")
        out_vrt = out_tif_full.replace(".tif", ".vrt")
        out_dir = os.path.dirname(out_vrt)

        if os.path.exists(out_vrt) and not self.overwrite:
            logging.getLogger(__name__).info("%s already exists; ending process" % out_vrt)
            return

        if not self.darker or sensor_type != 'rgb':
            (nu_created, nu_bytes) = self.generateSingleMosaic(connector, host, secret_key,
                                                               out_dir, out_vrt, out_tif_thumb, out_tif_full, parameters)
        else:
            (nu_created, nu_bytes) = self.generateDarkerMosaic(connector, host, secret_key,
                                                               out_dir, out_vrt, out_tif_thumb, out_tif_full, parameters)
        self.created += nu_created
        self.bytes += nu_bytes

        # Get dataset ID or create it, creating parent collections as needed
        target_dsid = build_dataset_hierarchy(host, secret_key, self.clowder_user, self.clowder_pass, self.clowderspace,
                                              self.sensors.get_display_name(), timestamp[:4],
                                              timestamp[5:7], leaf_ds_name=dataset_name)

        # Upload full field image to Clowder
        thumbid = upload_to_dataset(connector, host, self.clowder_user, self.clowder_pass, target_dsid, out_tif_thumb)
        fullid = upload_to_dataset(connector, host, self.clowder_user, self.clowder_pass, target_dsid, out_tif_full)

        content = {
            "comment": "This stitched image is computed based on an assumption that the scene is planar. \
                There are likely to be be small offsets near the boundary of two images anytime there are plants \
                at the boundary (because those plants are higher than the ground plane), or where the dirt is \
                slightly higher or lower than average.",
            "file_ids": parameters["file_paths"]
        }
        thumbmeta = build_metadata(host, self.extractor_info, thumbid, content, 'file')
        upload_metadata(connector, host, secret_key, thumbid, thumbmeta)
        fullmeta = build_metadata(host, self.extractor_info, fullid, content, 'file')
        upload_metadata(connector, host, secret_key, fullid, fullmeta)

        self.end_message()

    def generateSingleMosaic(self, connector, host, secret_key, out_dir, out_vrt, out_tif_thumb, out_tif_full, parameters):
        # Create simple mosaic from geotiff list
        created, bytes = 0, 0

        logging.getLogger(__name__).info("Target output directory: %s" % out_dir)

        if (not os.path.isfile(out_vrt)) or self.overwrite:
            fileidpath = self.remapMountPath(connector, str(parameters['file_paths']))
            with open(fileidpath) as flist:
                file_path_list = json.load(flist)
            logging.getLogger(__name__).info("Preparing %s TIFs from %s" % (len(file_path_list), str(parameters['file_paths'])))

            # Write input list to tmp file
            tiflist = "tiflist.txt"
            with open(tiflist, "w") as tifftxt:
                for tpath in file_path_list:
                    filepath = self.remapMountPath(connector, tpath)
                    tifftxt.write("%s\n" % filepath)

            # Create VRT from every GeoTIFF
            logging.getLogger(__name__).info("Creating %s..." % out_vrt)
            full_day_to_tiles.createVrtPermanent(out_dir, tiflist, out_vrt)
            os.remove(tiflist)
            created += 1
            bytes += os.path.getsize(out_vrt)

        if (not os.path.isfile(out_tif_thumb)) or self.overwrite:
            # Convert VRT to full-field GeoTIFF (low-res then high-res)
            logging.getLogger(__name__).info("Converting VRT to %s..." % out_tif_thumb)

            cmd = "gdal_translate -projwin -111.9750963 33.0764953 -111.9747967 33.074485715 " + \
                    "-outsize 4%% 4%% %s %s" % (out_vrt, out_tif_thumb)
            subprocess.call(cmd, shell=True)
            created += 1
            bytes += os.path.getsize(out_tif_thumb)

        """
        if (not os.path.isfile(out_tif_full)) or self.overwrite:
            logging.getLogger(__name__).info("Converting VRT to %s..." % out_tif_full)
            cmd = "gdal_translate -projwin -111.9750963 33.0764953 -111.9747967 33.074485715 " + \
                    "%s %s" % (out_vrt, out_tif_full)
            subprocess.call(cmd, shell=True)
            created += 1
            bytes += os.path.getsize(out_tif_full)
        """

        return (created, bytes)

    def generateDarkerMosaic(self, connector, host, secret_key, out_dir, out_vrt, out_tif_thumb, out_tif_full, parameters):
        # Create dark-pixel mosaic from geotiff list using multipass for darker pixel selection
        created, bytes = 0, 0

        if (not os.path.isfile(out_vrt)) or self.overwrite:
            fileidpath = self.remapMountPath(connector, str(parameters['file_paths']))
            with open(fileidpath) as flist:
                file_path_list = json.load(flist)
            logging.getLogger(__name__).info("processing %s TIFs with dark flag" % len(file_path_list))

            # Write input list to tmp file
            tiflist = "tiflist.txt"
            with open(tiflist, "w") as tifftxt:
                for tpath in file_path_list:
                    filepath = self.remapMountPath(connector, tpath)
                    tifftxt.write("%s\n" % filepath)

            # Create VRT from every GeoTIFF
            logging.getLogger(__name__).info("Creating %s..." % out_vrt)
            full_day_to_tiles.createVrtPermanent(out_dir, tiflist, out_vrt)
            created += 1
            bytes += os.path.getsize(out_vrt)

            # Split full tiflist into parts according to split number
            shade.split_tif_list(tiflist, out_dir, self.split)

            # Generate tiles from each split VRT into numbered folders
            shade.create_diff_tiles_set(out_dir, self.split)

            # Choose darkest pixel from each overlapping tile
            unite_tiles_dir = os.path.join(out_dir, 'unite')
            if not os.path.exists(unite_tiles_dir):
                os.mkdir(unite_tiles_dir)
            shade.integrate_tiles(out_dir, unite_tiles_dir, self.split)

            # If any files didn't have overlap, copy individual tile
            shade.copy_missing_tiles(out_dir, unite_tiles_dir, self.split, tiles_folder_name='tiles_left')

            # Create output VRT from overlapped tiles
            shade.create_unite_tiles(unite_tiles_dir, out_vrt)
            created += 1
            bytes += os.path.getsize(out_vrt)

        if (not os.path.isfile(out_tif_thumb)) or self.overwrite:
            # Convert VRT to full-field GeoTIFF (low-res then high-res)
            logging.getLogger(__name__).info("Converting VRT to %s..." % out_tif_thumb)
            subprocess.call("gdal_translate -projwin -111.9750963 33.0764953 -111.9747967 33.074485715 "+
                             "-outsize 10% 10% %s %s" % (out_vrt, out_tif_thumb), shell=True)
            created += 1
            bytes += os.path.getsize(out_tif_thumb)

        if (not os.path.isfile(out_tif_full)) or self.overwrite:
            logging.getLogger(__name__).info("Converting VRT to %s..." % out_tif_full)
            subprocess.call("gdal_translate -projwin -111.9750963 33.0764953 -111.9747967 33.074485715 "+
                             "%s %s" % (out_vrt, out_tif_full), shell=True)
            created += 1
            bytes += os.path.getsize(out_tif_full)

        return (created, bytes)

    def remapMountPath(self, connector, path):
        if len(connector.mounted_paths) > 0:
            for source_path in connector.mounted_paths:
                if path.startswith(source_path):
                    return path.replace(source_path, connector.mounted_paths[source_path])
            return path
        else:
            return path

if __name__ == "__main__":
    extractor = FullFieldMosaicStitcher()
    extractor.start()
