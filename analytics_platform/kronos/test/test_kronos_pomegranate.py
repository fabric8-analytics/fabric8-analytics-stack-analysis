import logging

from analytics_platform.kronos.src import config
from analytics_platform.kronos.src.kronos_pomegranate import KronosPomegranate
from analytics_platform.kronos.src.offline_training import load_eco_to_kronos_dependency_dict, \
    load_user_eco_to_co_occerrence_matrix_dict
from analytics_platform.kronos.src.online_scoring import score_eco_user_package_dict, \
    load_user_eco_to_kronos_model_dict, get_eco_to_kronos_dependency_dict
from util.data_store.local_filesystem import LocalFileSystem

logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)

from unittest import TestCase


class TestKronosPomegranate(TestCase):
    def test_train_and_save_kronos_list_local(self):

        input_data_store = LocalFileSystem("analytics_platform/kronos/test/data/input-train-data")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem("analytics_platform/kronos/test/data/output-train-data")
        self.assertTrue(output_data_store is not None)

        eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict(
            input_kronos_dependency_data_store=input_data_store)
        self.assertTrue(eco_to_kronos_dependency_dict is not None)

        user_eco_to_cooccurrence_matrix_dict = load_user_eco_to_co_occerrence_matrix_dict(
            input_co_occurrence_data_store=input_data_store)
        self.assertTrue(user_eco_to_cooccurrence_matrix_dict is not None)

        for user_category in user_eco_to_cooccurrence_matrix_dict.keys():
            eco_to_cooccurrence_matrix_dict = user_eco_to_cooccurrence_matrix_dict[user_category]
            for ecosystem in eco_to_cooccurrence_matrix_dict.keys():
                kronos_dependency_dict = eco_to_kronos_dependency_dict[ecosystem]
                cooccurrence_matrix_df = eco_to_cooccurrence_matrix_dict[ecosystem]
                kronos_model = KronosPomegranate.train(kronos_dependency_dict=kronos_dependency_dict,
                                                       package_occurrence_df=cooccurrence_matrix_df)
                self.assertTrue(kronos_model is not None)
                filename = "data_kronos_user_eco" + "/" + str(user_category) + "/" + "kronos" + "_" + str(
                    ecosystem) + ".json"
                kronos_model.save(data_store=output_data_store, filename=filename)

    def test_score_eco_user_package_dict(self):
        input_data_store = LocalFileSystem("analytics_platform/kronos/test/data/input-score-data")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem("analytics_platform/kronos/test/data/output-score-data")
        self.assertTrue(output_data_store is not None)

        user_eco_kronos_dict = load_user_eco_to_kronos_model_dict(input_kronos_data_store=input_data_store)

        self.assertTrue(user_eco_kronos_dict is not None)

        eco_to_kronos_dependency_dict = get_eco_to_kronos_dependency_dict(data_store=input_data_store,
                                                                          folderpath="data_kronos_dependency")
        self.assertTrue(eco_to_kronos_dependency_dict is not None)

        user_request = [{"ecosystem": "pypi", "user_persona": "1", "package_list": [
            "p1",
            "p2",
            "p3",
            "p4", "p6"
        ]}]

        response = score_eco_user_package_dict(user_request, user_eco_kronos_dict=user_eco_kronos_dict,
                                               eco_to_kronos_dependency_dict=eco_to_kronos_dependency_dict,
                                               comp_package_count_threshold=10, alt_package_count_threshold=5,
                                               outlier_threshold=0.61)
        self.assertTrue(response is not None)

        output_data_store.write_json_file(filename="response.json", contents=response)

        expected_response = output_data_store.read_json_file(filename="expected_response.json")
        self.assertTrue(expected_response is not None)

        self.assertDictEqual(response[0], expected_response[0])
