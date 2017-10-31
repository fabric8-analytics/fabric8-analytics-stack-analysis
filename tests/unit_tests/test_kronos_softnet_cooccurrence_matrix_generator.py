import logging

from pandas.util.testing import assert_frame_equal

from analytics_platform.kronos.src import config
from analytics_platform.kronos.softnet.src.cooccurrence_matrix_generator import CooccurrenceMatrixGenerator
from analytics_platform.kronos.softnet.src.offline_training import load_eco_to_kronos_dependency_dict
from util.data_store.local_filesystem import LocalFileSystem

logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)

from unittest import TestCase


class TestCooccurrenceMatrixGenerator(TestCase):

    def test_generate_and_save_cooccurrence_matrices_local(self):
        input_data_store = LocalFileSystem(
            "tests/data/data_softnet/input-com-data")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem(
            "tests/data/data_softnet/output-com-data")
        self.assertTrue(output_data_store is not None)

        eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict(
            input_kronos_dependency_data_store=input_data_store,additional_path="")
        self.assertTrue(eco_to_kronos_dependency_dict is not None)

        manifest_filenames = input_data_store.list_files("data_input_manifest_file_list")
        self.assertTrue(manifest_filenames is not None)

        for manifest_filename in manifest_filenames:
            user_category = manifest_filename.split("/")[1]
            manifest_content_json_list = input_data_store.read_json_file(filename=manifest_filename)
            self.assertTrue(manifest_content_json_list is not None)

            for manifest_content_json in manifest_content_json_list:
                self.assertTrue(manifest_content_json is not None)
                manifest_content_dict = dict(manifest_content_json)
                ecosystem = manifest_content_dict["ecosystem"]
                kronos_dependency_dict = eco_to_kronos_dependency_dict[ecosystem]
                list_of_package_list = manifest_content_dict.get("package_list")
                cooccurrence_matrix_obj = CooccurrenceMatrixGenerator.generate_cooccurrence_matrix(
                    kronos_dependency_dict=kronos_dependency_dict, list_of_package_list=list_of_package_list)
                self.assertTrue(cooccurrence_matrix_obj is not None)
                output_filename = "data_co_occurrence_matrix" + "/" + str(
                    user_category) + "/" + "cooccurrence_matrix" + "_" + str(
                    ecosystem) + ".json"
                cooccurrence_matrix_obj.save(data_store=output_data_store, filename=output_filename)
                expected_output_filename = "data_co_occurrence_matrix" + "/" + str(
                    user_category) + "/" + "expected_cooccurrence_matrix" + "_" + str(
                    ecosystem) + ".json"
                expected_cooccurrence_matrix_obj = CooccurrenceMatrixGenerator.load(data_store=output_data_store,
                                                                                    filename=expected_output_filename)
                self.assertTrue(expected_cooccurrence_matrix_obj is not None)
                cooccurrence_matrix_df = cooccurrence_matrix_obj.get_matrix_dictionary()
                self.assertTrue(cooccurrence_matrix_df is not None)
                expected_cooccurrence_matrix_df = expected_cooccurrence_matrix_obj.get_matrix_dictionary()
                expected_columns = set(expected_cooccurrence_matrix_df.columns)
                resultant_columns = set(cooccurrence_matrix_df.columns)
                self.assertTrue(resultant_columns == expected_columns)
                self.assertTrue(set(cooccurrence_matrix_df).issubset(
                    set(expected_cooccurrence_matrix_df)))
