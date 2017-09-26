import logging

from analytics_platform.kronos.src import config
from analytics_platform.kronos.gnosis.src.gnosis_ref_arch import GnosisReferenceArchitecture
from util.data_store.local_filesystem import LocalFileSystem

logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)

from unittest import TestCase


# TODO: Write a deep dictionary comparator


class TestGnosisReferenceArchitecture(TestCase):

    def test_train_and_save_gnosis_ref_arch_local(self):
        input_data_store = LocalFileSystem(
            "tests/data/data_gnosis/input-ra-data")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem(
            "tests/data/data_gnosis/output-ra-data")
        self.assertTrue(output_data_store is not None)

        gnosis_ra_obj = GnosisReferenceArchitecture.train(data_store=input_data_store, min_support_count=40,
                                                          min_intent_topic_count=2,fp_num_partition=12)

        self.assertTrue(gnosis_ra_obj is not None)
        output_result = gnosis_ra_obj.get_dictionary()

        self.assertTrue(output_result is not None)

        expected_gnosis_ra_obj = GnosisReferenceArchitecture.load(data_store=output_data_store,
                                                                  filename="data_gnosis/expected_gnosis_ref_arch.json")

        self.assertTrue(expected_gnosis_ra_obj is not None)

        expected_output_result = expected_gnosis_ra_obj.get_dictionary()

        self.assertTrue(expected_output_result is not None)

        self.assertDictEqual(output_result, expected_output_result)

        gnosis_ra_obj.save(data_store=output_data_store, filename="data_gnosis/gnosis_ref_arch.json")
