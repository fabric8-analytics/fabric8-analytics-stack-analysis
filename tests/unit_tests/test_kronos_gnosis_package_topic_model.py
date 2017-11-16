import logging

from analytics_platform.kronos.src import config
from analytics_platform.kronos.gnosis.src.gnosis_package_topic_model import GnosisPackageTopicModel
from util.data_store.local_filesystem import LocalFileSystem

logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)

from unittest import TestCase


class TestGnosisPackageTopicModel(TestCase):

    def test_generate_and_save_package_topic_model_local(self):
        input_data_store = LocalFileSystem(
            "tests/data/data_gnosis/input-ptm-data")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem(
            "tests/data/data_gnosis/output-ptm-data")
        self.assertTrue(output_data_store is not None)

        package_topic_model = GnosisPackageTopicModel.curate(
            data_store=input_data_store,
            filename="data_input_curated_package_topic/package_topic.json")

        self.assertTrue(package_topic_model is not None)
        output_result = package_topic_model.get_dictionary()

        self.assertTrue(output_result is not None)

        expected_package_topic_model = GnosisPackageTopicModel.load(
            data_store=output_data_store,
            filename="data_package_topic/expected_package_topic.json")

        self.assertTrue(expected_package_topic_model is not None)

        expected_output_result = expected_package_topic_model.get_dictionary()

        self.assertTrue(expected_output_result is not None)
        self.assertDictEqual(output_result, expected_output_result)

        package_topic_model.save(data_store=output_data_store,
                                 filename="data_package_topic/package_topic.json")

    def test_manifest_missing_packages(self):
        input_data_store = LocalFileSystem(
            "tests/data/data_gnosis")
        self.assertTrue(input_data_store is not None)
        manifest_json = input_data_store.read_json_file(
            filename='data_input_manifest_file_list/manifest_unknown_packages.json'
        )
        self.assertTrue(manifest_json)
        self.assertTrue("package_list" in manifest_json[0])
        package_list = manifest_json[0]['package_list']
        packages = GnosisPackageTopicModel._get_unknown_packages_from_manifests(
            input_data_store,
            additional_path='',
            package_topic_dict={}
        )
        self.assertListEqual(sorted(package_list[0]), sorted(packages.keys()))

    def test_package_tag_creation(self):
        input_data_store = LocalFileSystem(
            "tests/data/data_gnosis/input-ptm-data")
        self.assertTrue(input_data_store is not None)
        ptm_json = input_data_store.read_json_file(
            filename='data_input_curated_package_topic/package_topic.json')
        self.assertTrue(ptm_json)
        package_names = ptm_json[0]['package_topic_map']
        for package_name in package_names:
            tag_list = GnosisPackageTopicModel._create_tags_for_package(
                package_name)
            # At least one tag should be generated for each package
            self.assertTrue(tag_list)
