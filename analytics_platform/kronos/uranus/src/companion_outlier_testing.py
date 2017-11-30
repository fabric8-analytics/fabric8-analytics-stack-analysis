# NOTE: Works only with S3DataStore
from itertools import combinations
import time

from analytics_platform.kronos.src.config import (
    AWS_BUCKET_NAME,
    KRONOS_MODEL_PATH,
    KRONOS_SCORING_REGION)
from analytics_platform.kronos.src.kronos_online_scoring import (
    load_user_eco_to_kronos_model_dict_s3,
    score_eco_user_package_dict)
from analytics_platform.kronos.uranus.src.uranus_constants import (
    COMPANION_COUNT_THRESHOLD,
    ALTERNATE_COUNT_THRESHOLD,
    OUTLIER_PROBABILITY_THRESHOLD,
    UNKNOWN_PROBABILITY_THRESHOLD)
from analytics_platform.kronos.uranus.src.super_class import Accuracy


class CompanionOutlierAccuracy(Accuracy):

    def __init__(self):
        super(CompanionOutlierAccuracy, self).__init__()
        self.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3(
            bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.test_set = set()
        self.unique_items_len = 0

    def predict_and_score(self, input_json):
        """Call the scoring fcuntion of Kronos Online Scoring.

        :return: The recommendation JSON result."""

        return score_eco_user_package_dict(
            user_request=input_json,
            user_eco_kronos_dict=self.user_eco_kronos_dict,
            eco_to_kronos_dependency_dict=self.eco_to_kronos_dependency_dict,
            all_package_list_obj=None)

    @staticmethod
    def create_input_dict(package_list):
        """Generate the input JSON required by Kronos scoring.

        :param package_list: The list containing user stack.

        :return: Required input JSON."""

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

    def generate_test_set(self):
        """Generate the test data set using freq item sets."""

        freq_items_5 = self.generate_freq_items(5)
        unique_pck = set()

        for each_item_list in freq_items_5:
            for each_package in each_item_list:
                unique_pck.add(each_package)
            combinations_4 = list(combinations(each_item_list, 4))
            for each_combination in combinations_4:
                self.test_set.add(frozenset(each_combination))
        self.unique_items_len = float(len(unique_pck))

    def companion_outlier_precision(self):
        """Score each test set and generate the recommendations.
        For each companion recommendation check its presence in the search set.
        If companion subset matches increase the true positive counter
            else the false positive counter.
        For each outlier recommendation, increase the count for number of outliers found."""

        t0 = time.time()
        time_for_pgm = 0
        counter = 1
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
            print(counter)
            counter += 1

        print("\n")
        print(("For {} test cases it took {} seconds to test.".format(
            counter - 1, time.time() - t0)))
        print("\n")
        print(("Companion: True Positives = {}".format(true_positives)))
        print(("Companion: False Positives = {}".format(false_positives)))
        print(("Companion: Precision Percentage = {}".format(
            true_positives / (true_positives + false_positives) * 100)))
        print("\n")
        print(("Outlier: Number of outliers = {}".format(total_outliers)))
        print(("Outlier: Ratio of Outlier to Unique Freq. Items = {}".format(
            total_outliers / self.unique_items_len)))


if __name__ == '__main__':
    training_data_url = "s3://dev-stack-analysis-clean-data/maven/github/"
    co_acc_obj = CompanionOutlierAccuracy()
    co_acc_obj.load_package_list(training_data_url)
    co_acc_obj.generate_test_set()
    co_acc_obj.generate_whole_set()
    co_acc_obj.companion_outlier_precision()
