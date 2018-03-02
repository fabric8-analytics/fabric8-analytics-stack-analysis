"""Score each test set and generate the recommendations."""

# NOTE: Currently works only with S3DataStore
from itertools import combinations
import time
import os

from analytics_platform.kronos.src.config import (
    AWS_BUCKET_NAME,
    KRONOS_MODEL_PATH,
    KRONOS_SCORING_REGION)
from analytics_platform.kronos.src.kronos_online_scoring import (
    load_user_eco_to_kronos_model_dict_s3,
    score_eco_user_package_dict,
    load_package_frequency_dict_s3)
from evaluation_platform.uranus.src.uranus_constants import (
    COMPANION_COUNT_THRESHOLD,
    ALTERNATE_COUNT_THRESHOLD,
    OUTLIER_PROBABILITY_THRESHOLD,
    UNKNOWN_PROBABILITY_THRESHOLD,
    URANUS_OUTPUT_PATH)
from evaluation_platform.uranus.src.super_class import Accuracy
from analytics_platform.kronos.src.recommendation_validator import RecommendationValidator
import daiquiri
import logging

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


class CompanionOutlierAccuracy(Accuracy):
    """Score each test set and generate the recommendations."""

    def __init__(self):
        """Initialize this class."""
        super(CompanionOutlierAccuracy, self).__init__()
        self.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3(
            bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.package_frequency_dict = load_package_frequency_dict_s3(
            bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.all_package_list_obj = RecommendationValidator.load_package_list_s3(
            input_bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.test_set = set()
        self.unique_items_len = 0

    def load_attributes(self, input_data_store, additional_path):
        """Load the attributes of the class object.

        :param input_data_store: The place to fetch the data from.
        :param additional_path: The directory to pick the manifest files from.
        """
        complete_output_filename = os.path.join(
            additional_path, URANUS_OUTPUT_PATH, "comp_test_set.pickle")
        self.test_set = input_data_store.load_pickle_file(
            filename=complete_output_filename)
        self.unique_items_len = self.get_unique_item_len()
        self.load_search_set(input_data_store, additional_path)

    def predict_and_score(self, input_json):
        """Call the scoring fcuntion of Kronos Online Scoring.

        :return: The recommendation JSON result.
        """
        return score_eco_user_package_dict(
            user_request=input_json,
            user_eco_kronos_dict=self.user_eco_kronos_dict,
            eco_to_kronos_dependency_dict=self.eco_to_kronos_dependency_dict,
            all_package_list_obj=self.all_package_list_obj,
            package_frequency_dict=self.package_frequency_dict,
            use_filters=False)

    @staticmethod
    def create_input_dict(package_list):
        """Generate the input JSON required by Kronos scoring.

        :param package_list: The list containing user stack.

        :return: Required input JSON.
        """
        return [
            {
                "ecosystem": KRONOS_SCORING_REGION,
                "comp_package_count_threshold": COMPANION_COUNT_THRESHOLD,
                "alt_package_count_threshold": ALTERNATE_COUNT_THRESHOLD,
                "outlier_probability_threshold": OUTLIER_PROBABILITY_THRESHOLD,
                "unknown_packages_ratio_threshold": UNKNOWN_PROBABILITY_THRESHOLD,
                "package_list": package_list
            }
        ]

    def get_unique_item_len(self):
        """Return the number of unique packages present in the test set."""
        unique_count = 0
        for each_set in self.test_set:
            unique_count += len(each_set)
        return unique_count

    def companion_outlier_precision(self):
        """Score each test set and generate the recommendations.

        For each companion recommendation check its presence in the search set.
        If companion subset matches increase the true positive counter
            else the false positive counter.
        For each outlier recommendation, increase the count for number of outliers found.

        :return companion_precision_result: The evaluation result for companion packages.
        :return outliers_precision_result: The evaluation result for outliers packages.
        """
        companion_precision_result = {
            "Number of Test Cases": self.unique_items_len}
        outlier_precision_result = {
            "Number of Test Cases": self.unique_items_len}
        t0 = time.time()
        true_positives = 0.
        false_positives = 0.
        total_outliers = 0.
        for each_test_set in self.test_set:
            input_list = list(each_test_set)
            result = self.predict_and_score(
                self.create_input_dict(input_list))
            comp_list = [com_pck.get('package_name') for com_pck in result[
                0].get('companion_packages', {})]
            num_outliers_found = len(result[0].get('outlier_package_list', []))
            total_outliers += num_outliers_found
            for comp_pck in comp_list:
                temp_input = input_list[:]
                temp_input.append(comp_pck)
                if self.check_present(frozenset(temp_input)):
                    true_positives += 1
                else:
                    false_positives += 1
        time_taken = time.time() - t0

        companion_precision_result["Time taken(sec)"] = time_taken
        companion_precision_result["True Positives"] = true_positives
        companion_precision_result["False Positives"] = false_positives
        companion_precision_result[
            "Precision Percentage"] = true_positives / \
            (true_positives + false_positives) * 100

        outlier_precision_result["Time taken(sec)"] = time_taken
        outlier_precision_result["Number of Outliers"] = total_outliers
        outlier_precision_result[
            "Outlier Percentage"] = total_outliers / self.unique_items_len * 100

        _logger.info(
            "Companion and outlier testing ended in {} seconds".format(time_taken))
        _logger.info("Companion precision is {} %".format(
            companion_precision_result["Precision Percentage"]))
        _logger.info("Outlier Percentage is {}".format(outlier_precision_result[
            "Outlier Percentage"]))

        return companion_precision_result, outlier_precision_result
