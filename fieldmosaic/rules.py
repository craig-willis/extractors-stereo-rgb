#!/usr/bin/env python

import os
import logging
import subprocess
import json

import rule_utils
from terrautils.sensors import Sensors
from terrautils.metadata import get_terraref_metadata
from pyclowder.datasets import download_metadata


# setup logging for the exctractor
logging.getLogger('pyclowder').setLevel(logging.DEBUG)
logging.getLogger('__main__').setLevel(logging.DEBUG)


# This rule can be used with the rulechecker extractor to trigger the fieldmosaic extractor.
# https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/extractors-rulechecker
def fullFieldMosaicStitcher(extractor, connector, host, secret_key, resource, rulemap):
    results = {}
    full_field_ready = False

    # full-field queues must have at least this percent of the raw datasets present to trigger
    tolerance_pct = 100
    # full-field queues must have at least this many datasets to trigger
    min_datasets = 200

    # Determine output dataset
    dsname = resource["dataset_info"]["name"]
    sensor = dsname.split(" - ")[0]

    # Map sensor display names to the GeoTIFF stitching target in those sensor datasets,
    # including directory to look for date subfolder to count # of datasets on that date
    if os.path.exists('/projects/arpae/terraref/sites'):
        TERRAREF_BASE = '/projects/arpae/terraref/sites'
    elif os.path.exists('/home/clowder/sites'):
        TERRAREF_BASE = '/home/clowder/sites'
    else:
        TERRAREF_BASE = '/home/extractor/sites'

    sensor_lookup = Sensors(TERRAREF_BASE, 'ua-mac')
    stitchable_sensors = {
        sensor_lookup.get_display_name('rgb_geotiff'): {
            "target": "_left.tif",
            "raw_dir": os.path.join(*(sensor_lookup.get_sensor_path('', sensor='stereoTop').split("/")[:-2]))
        },
        sensor_lookup.get_display_name('ir_geotiff'): {
            "target": ".tif",
            "raw_dir": os.path.join(*(sensor_lookup.get_sensor_path('', sensor='flirIrCamera').split("/")[:-2]))
        },
        sensor_lookup.get_display_name('laser3d_heightmap'): {
            "target": "_west.tif",
            "raw_dir": os.path.join(*(sensor_lookup.get_sensor_path('', sensor='scanner3DTop').split("/")[:-2]))
        },
        'scanner3DTop': {
            "target": "_west.tif",
            "raw_dir": os.path.join(*(sensor_lookup.get_sensor_path('', sensor='scanner3DTop').split("/")[:-2]))
        }
    }

    if sensor in stitchable_sensors.keys():
        timestamp = dsname.split(" - ")[1]
        date = timestamp.split("__")[0]
        # Get the scan from the metadata so we can include in unique key
        ds_md = download_metadata(connector, host, secret_key, resource["dataset_info"]["id"])
        terra_md = get_terraref_metadata(ds_md)
        if 'gantry_variable_metadata' in terra_md and 'script_name' in terra_md['gantry_variable_metadata']:
            if terra_md['gantry_variable_metadata']["fullfield_eligible"] == "False":
                # Not full-field scan; no need to trigger anything for now.
                logging.info("%s is not a full-field eligible scan" % dsname)
                for trig_extractor in rulemap["extractors"]:
                    results[trig_extractor] = {
                        "process": False,
                        "parameters": {}
                    }
                return results
            target_scan = terra_md['gantry_variable_metadata']['script_name']
        else:
            target_scan = "unknown_scan"

        progress_key = "Full Field -- %s - %s (%s)" % (sensor, date, target_scan)
        group_key = "Full Field -- %s - %s" % (sensor, date)

        # Is there actually a new left geoTIFF to add to the stack?
        target_id = None
        for f in resource['files']:
            if f['filename'].endswith(stitchable_sensors[sensor]["target"]):
                target_id = f['id']
                target_path = f['filepath']
        if not target_id:
            # If not, no need to trigger anything for now.
            logging.info("no target geoTIFF found in %s" % dsname)
            for trig_extractor in rulemap["extractors"]:
                results[trig_extractor] = {
                    "process": False,
                    "parameters": {}
                }
            return results

        logging.info("[%s] found target: %s" % (progress_key, target_id))

        # Fetch all existing file IDs that would be fed into this field mosaic
        progress = rule_utils.retrieveProgressFromDB(progress_key)

        # Is current ID already included in the list? If not, add it
        submit_record = False
        if 'ids' in progress:
            ds_count = len(progress['ids'].keys())
            if target_id not in progress['ids'].keys():
                submit_record = True
                ds_count += 1
            else:
                # Already seen this geoTIFF, so skip for now.
                logging.info("previously logged target geoTIFF from %s" % dsname)
                for trig_extractor in rulemap["extractors"]:
                    results[trig_extractor] = {
                        "process": False,
                        "parameters": {}
                    }
        else:
            submit_record = True
            ds_count = 1

        if submit_record:
            for trig_extractor in rulemap["extractors"]:
                rule_utils.submitProgressToDB("fullFieldMosaicStitcher", trig_extractor, progress_key, target_id, target_path)

        if ds_count >= min_datasets:
            # Check to see if list of geotiffs is same length as list of raw datasets
            root_dir = stitchable_sensors[sensor]["raw_dir"]
            if len(connector.mounted_paths) > 0:
                for source_path in connector.mounted_paths:
                    if root_dir.startswith(source_path):
                        root_dir = root_dir.replace(source_path, connector.mounted_paths[source_path])
            date_directory = os.path.join(root_dir, date)
            date_directory = ("/"+date_directory if not date_directory.startswith("/") else "")
            raw_file_count = float(subprocess.check_output("ls %s | wc -l" % date_directory, shell=True).strip())

            if raw_file_count == 0:
                raise Exception("problem communicating with file system")
            else:
                # If we have enough raw files accounted for and more than min_datasets, trigger
                counts_for_all_scans = float(rule_utils.retrieveQueryCountsFromDB(group_key)["total"])
                prog_pct = float(counts_for_all_scans/raw_file_count)*100
                if prog_pct >= tolerance_pct:
                    full_field_ready = True
                else:
                    logging.info("found %s/%s (%s%%) necessary geotiffs (%s in this scan)" % (counts_for_all_scans, int(raw_file_count),
                                                                            "{0:.2f}".format(prog_pct),  len(progress['ids'])))
        for trig_extractor in rulemap["extractors"]:
            results[trig_extractor] = {
                "process": full_field_ready,
                "parameters": {}
            }
            if full_field_ready:
                results[trig_extractor]["parameters"]["output_dataset"] = "Full Field - "+date
                results[trig_extractor]["parameters"]["scan_type"] = target_scan

                # Write output ID list to a text file
                output_dir = os.path.dirname(sensor_lookup.get_sensor_path(date, 'fullfield'))
                logging.info("writing %s_file_ids.json to %s" % (sensor, output_dir))
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                output_file = os.path.join(output_dir, sensor+"_"+target_scan+"_file_paths.json")

                # Sort IDs by file path before writing to disk
                # TODO: Eventually alternate every other image so we have half complete and half "underneath"
                paths = []
                for fid in progress['ids'].keys():
                    paths.append(progress['ids'][fid])
                with open(output_file, 'w') as out:
                    json.dump(sorted(paths), out)
                results[trig_extractor]["parameters"]["file_paths"] = output_file

    else:
        for trig_extractor in rulemap["extractors"]:
            results[trig_extractor] = {
                "process": False,
                "parameters": {}
            }

    return results