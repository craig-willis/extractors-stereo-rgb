import unittest
import os
import json
import tempfile
import shutil
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import bin_to_geotiff as bin2tiff

from terrautils.metadata import clean_metadata
from terrautils.metadata import get_terraref_metadata

from terrautils.formats import create_geotiff

from terrautils.spatial import geojson_to_tuples


test_id = 'aa2ffdb2-4b44-4828-ae3c-9be5698241ca'
path = os.path.join(os.path.dirname(__file__), test_id)


class TestBin2tif(unittest.TestCase):
    def test_bin2tif(self):

        f = open(path + '_metadata.json', 'rb')
        raw_metadata = json.load(f)
        f.close()

        cleanmetadata = clean_metadata(raw_metadata, "stereoTop")
        self.assertTrue('sensor_variable_metadata' in cleanmetadata.keys())

        # Find specific TERRA subset of metadata (to ignore other kinds)
        metadata = get_terraref_metadata(cleanmetadata, 'stereoTop')
        self.assertTrue('terraref_cleaned_metadata' in metadata.keys())

        img_left = path + '_left.bin'

        left_shape = bin2tiff.get_image_shape(metadata, 'left')
        self.assertTrue(left_shape == (3296, 2472))

        left_gps_bounds = geojson_to_tuples(metadata['spatial_metadata']['left']['bounding_box'])

        out_tmp_tiff = os.path.join(tempfile.gettempdir(), test_id.encode('utf8'))
        left_image = bin2tiff.process_image(left_shape, img_left, None)

        f = open('../extractor_info.json', 'rb')
        extractor_info = json.load(f)
        f.close()

        create_geotiff(left_image, left_gps_bounds, out_tmp_tiff, None, False, extractor_info, metadata)
        shutil.move(out_tmp_tiff, os.path.dirname(__file__)+'/test_result.tif')

        self.assertTrue(os.path.isfile(os.path.dirname(__file__) + '/test_result.tif'))


if __name__ == '__main__':
    unittest.main()
