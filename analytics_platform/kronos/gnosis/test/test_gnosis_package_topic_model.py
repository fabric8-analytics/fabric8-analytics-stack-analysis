import logging

from analytics_platform.kronos.src import config
from analytics_platform.kronos.gnosis.src.gnosis_package_topic_model import GnosisPackageTopicModel
from util.data_store.local_filesystem import LocalFileSystem

logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)

from unittest import TestCase


class TestGnosisPackageTopicModel(TestCase):
    def test_generate_and_save_package_topic_model_local(self):
        input_data_store = LocalFileSystem("analytics_platform/kronos/gnosis/test/data/input-ptm-data")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem("analytics_platform/kronos/gnosis/test/data/output-ptm-data")
        self.assertTrue(output_data_store is not None)

        package_topic_model = GnosisPackageTopicModel.curate(data_store=input_data_store,
                                                             filename="data_input_curated_package_topic/package_topic.json")
        self.assertTrue(package_topic_model is not None)
        output_result = package_topic_model.get_dictionary()

        self.assertTrue(output_result is not None)

        expected_package_topic_model = GnosisPackageTopicModel.load(data_store=output_data_store,
                                                                    filename="data_package_topic/expected_package_topic.json")

        self.assertTrue(expected_package_topic_model is not None)

        expected_output_result = expected_package_topic_model.get_dictionary()

        self.assertTrue(expected_output_result is not None)

        self.assertDictEqual(output_result, expected_output_result)

        package_topic_model.save(data_store=output_data_store, filename="data_package_topic/package_topic.json")
