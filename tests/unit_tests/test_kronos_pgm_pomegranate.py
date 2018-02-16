from analytics_platform.kronos.pgm.src.pgm_pomegranate import PGMPomegranate
from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict, \
    load_user_eco_to_co_occerrence_matrix_dict
from analytics_platform.kronos.src.kronos_online_scoring import score_eco_user_package_dict, \
    load_user_eco_to_kronos_model_dict, get_eco_to_kronos_dependency_dict
from analytics_platform.kronos.pgm.src.pgm_constants import KD_PACKAGE_FREQUENCY
from analytics_platform.kronos.src.recommendation_validator import RecommendationValidator
from util.data_store.local_filesystem import LocalFileSystem
from analytics_platform.kronos.src.config import USE_FILTERS
from unittest import TestCase
import os


class TestKronosPomegranate(TestCase):

    def test_train_and_save_kronos_list_local(self):

        input_data_store = LocalFileSystem(
            "tests/data/data_pgm/input-train-data/")
        self.assertIsNotNone(input_data_store)

        output_data_store = LocalFileSystem(
            "tests/data/data_pgm/output-train-data/")
        self.assertIsNotNone(output_data_store)

        eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict(
            input_kronos_dependency_data_store=input_data_store, additional_path="")
        self.assertIsNotNone(eco_to_kronos_dependency_dict)

        user_eco_to_cooccurrence_matrix_dict = load_user_eco_to_co_occerrence_matrix_dict(
            input_co_occurrence_data_store=input_data_store, additional_path="")
        self.assertIsNotNone(user_eco_to_cooccurrence_matrix_dict)

        for user_category in user_eco_to_cooccurrence_matrix_dict.keys():
            eco_to_cooccurrence_matrix_dict = user_eco_to_cooccurrence_matrix_dict[
                user_category]
            for ecosystem in eco_to_cooccurrence_matrix_dict.keys():
                kronos_dependency_dict = eco_to_kronos_dependency_dict[
                    ecosystem]
                cooccurrence_matrix_df = eco_to_cooccurrence_matrix_dict[
                    ecosystem]
                kronos_model = PGMPomegranate.train(kronos_dependency_dict=kronos_dependency_dict,
                                                    package_occurrence_df=cooccurrence_matrix_df)
                self.assertIsNotNone(kronos_model)
                filename = os.path.join("data_kronos_user_eco", str(user_category), "kronos" +
                                        "_" + str(ecosystem) + ".json")
                kronos_model.save(
                    data_store=output_data_store, filename=filename)

    def test_score_eco_user_package_dict(self):
        input_data_store = LocalFileSystem(
            "tests/data/data_pgm/input-score-data/")
        self.assertIsNotNone(input_data_store)

        output_data_store = LocalFileSystem(
            "tests/data/data_pgm/output-score-data/")
        self.assertIsNotNone(output_data_store)

        frequency_dict_data_store = LocalFileSystem(
            src_dir="tests/data/data_apollo/"
        )
        self.assertIsNotNone(frequency_dict_data_store)

        user_eco_kronos_dict = load_user_eco_to_kronos_model_dict(
            input_kronos_data_store=input_data_store, additional_path="")

        self.assertIsNotNone(user_eco_kronos_dict)

        eco_to_kronos_dependency_dict = get_eco_to_kronos_dependency_dict(
            data_store=input_data_store,
            folderpath="data_kronos_dependency")

        self.assertIsNotNone(eco_to_kronos_dependency_dict)

        user_request = [{"ecosystem": "pypi", "comp_package_count_threshold": 10,
                         "alt_package_count_threshold": 1,
                         "outlier_probability_threshold": 0.61,
                         "unknown_packages_ratio_threshold": 0.4,
                         "outlier_package_count_threshold": 2,
                         "package_list": [
                             "p1",
                             "p2",
                             "p3",
                             "np1"
                         ]}]

        frequency_dict = frequency_dict_data_store.read_json_file(filename=KD_PACKAGE_FREQUENCY)
        self.assertIsNotNone(frequency_dict)
        all_package_list_obj = RecommendationValidator.load_package_list_local(
            input_folder_name="tests/data/data_recom_valid/",
            additional_path="")

        response = score_eco_user_package_dict(
            user_request,
            user_eco_kronos_dict=user_eco_kronos_dict,
            eco_to_kronos_dependency_dict=eco_to_kronos_dependency_dict,
            all_package_list_obj=all_package_list_obj,
            package_frequency_dict=frequency_dict,
            use_filters=USE_FILTERS)

        self.assertIsNotNone(response)

        output_data_store.write_json_file(
            filename="response.json", contents=response)

        expected_response = output_data_store.read_json_file(
            filename="expected_response.json")
        self.assertIsNotNone(expected_response)

        self.assertDictEqual(response[0], expected_response[0])

    def test_score_user_eco_package_dict_with_duplicates(self):
        input_data_store = LocalFileSystem(
            "tests/data/data_pgm/input-score-data/")
        self.assertIsNotNone(input_data_store)

        output_data_store = LocalFileSystem(
            "tests/data/data_pgm/output-score-data/")
        self.assertIsNotNone(output_data_store)

        user_eco_kronos_dict = load_user_eco_to_kronos_model_dict(
            input_kronos_data_store=input_data_store, additional_path="")

        self.assertIsNotNone(user_eco_kronos_dict)

        eco_to_kronos_dependency_dict = get_eco_to_kronos_dependency_dict(
            data_store=input_data_store,
            folderpath="data_kronos_dependency")

        self.assertIsNotNone(eco_to_kronos_dependency_dict)

        user_request = [{"ecosystem": "pypi", "comp_package_count_threshold": 10,
                         "alt_package_count_threshold": 1,
                         "outlier_probability_threshold": 0.61,
                         "unknown_packages_ratio_threshold": 0.4,
                         "outlier_package_count_threshold": 2,
                         "package_list": [
                             "p1",
                             "p2",
                             "p3",
                             "np1",
                             "p2",
                             "p3",
                             "p1"
                         ]}]

        frequency_dict_data_store = LocalFileSystem(
            src_dir="tests/data/data_apollo/"
        )
        self.assertIsNotNone(frequency_dict_data_store)

        frequency_dict = frequency_dict_data_store.read_json_file(
            filename=KD_PACKAGE_FREQUENCY)
        all_package_list_obj = RecommendationValidator.load_package_list_local(
            input_folder_name="tests/data/data_recom_valid/",
            additional_path="")

        response = score_eco_user_package_dict(
            user_request, user_eco_kronos_dict=user_eco_kronos_dict,
            eco_to_kronos_dependency_dict=eco_to_kronos_dependency_dict,
            all_package_list_obj=all_package_list_obj,
            package_frequency_dict=frequency_dict,
            use_filters=USE_FILTERS)

        self.assertIsNotNone(response)

        output_data_store.write_json_file(
            filename="response.json", contents=response)

        expected_response = output_data_store.read_json_file(
            filename="expected_response.json")
        self.assertIsNotNone(expected_response)

        self.assertDictEqual(response[0], expected_response[0])
