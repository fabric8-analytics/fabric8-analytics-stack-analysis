import logging

from analytics_platform.kronos.src import config
from analytics_platform.kronos.softnet.src.kronos_dependency_generator import KronosDependencyGenerator
from util.data_store.local_filesystem import LocalFileSystem

logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)

from unittest import TestCase


class TestKronosDependencyGenerator(TestCase):
    def test_generate_and_save_kronos_dependency_local(self):
        input_data_store = LocalFileSystem("analytics_platform/kronos/softnet/test/data/input-kd-data")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem("analytics_platform/kronos/softnet/test/data/output-kd-data")
        self.assertTrue(output_data_store is not None)

        gnosis_ref_arch_json = input_data_store.read_json_file(filename="data_gnosis/gnosis_ref_arch.json")
        self.assertTrue(gnosis_ref_arch_json is not None)

        gnosis_ref_arch_dict = dict(gnosis_ref_arch_json)
        self.assertTrue(gnosis_ref_arch_dict is not None)

        package_topic_json = input_data_store.read_json_file("data_package_topic/package_topic.json")
        self.assertTrue(package_topic_json is not None)

        package_topic_dict = dict(package_topic_json)
        self.assertTrue(package_topic_dict is not None)

        eco_to_package_topic_dict = package_topic_dict["package_topic_map"]
        eco_to_topic_package_dict = package_topic_dict["topic_package_map"]

        eco_to_kronos_dependency_dict = dict()

        for ecosystem in eco_to_package_topic_dict.keys():
            package_to_topic_dict = eco_to_package_topic_dict.get(ecosystem)
            topic_to_package_dict = eco_to_topic_package_dict.get(ecosystem)

            kronos_dependency_obj = KronosDependencyGenerator.generate_kronos_dependency(
                gnosis_ref_arch_dict=gnosis_ref_arch_dict,
                package_to_topic_dict=package_to_topic_dict,
                topic_to_package_dict=topic_to_package_dict)

            self.assertTrue(kronos_dependency_obj is not None)

            eco_to_kronos_dependency_dict[ecosystem] = kronos_dependency_obj

        for ecosystem in eco_to_kronos_dependency_dict.keys():
            kronos_dependency_obj = eco_to_kronos_dependency_dict[ecosystem]
            filename = "data_kronos_dependency/kronos_dependency.json"
            filename_formatted = filename.replace(".", "_" + ecosystem + ".")
            kronos_dependency_obj.save(data_store=output_data_store, filename=filename_formatted)

            kronos_dependency_dict = kronos_dependency_obj.get_dictionary()

            self.assertTrue(kronos_dependency_dict is not None)

            expected_filename_formatted = filename_formatted.replace("/", "/expected_")

            expected_kronos_dependency_obj = KronosDependencyGenerator.load(data_store=output_data_store,
                                                                            filename=expected_filename_formatted)
            self.assertTrue(expected_kronos_dependency_obj is not None)

            expected_kronos_dependency_dict = expected_kronos_dependency_obj.get_dictionary()
            self.assertTrue(expected_kronos_dependency_dict is not None)

            self.assertDictEqual(kronos_dependency_dict, expected_kronos_dependency_dict)
