# NOTE: Works only with S3DataStore
from __future__ import division
from analytics_platform.kronos.src.config import (
    SPARK_HOME_PATH,
    PY4J_VERSION,
    AWS_BUCKET_NAME,
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY,
    KRONOS_MODEL_PATH,
    KRONOS_SCORING_REGION)
import os
import itertools
import time
import sys
sys.path.insert(0, SPARK_HOME_PATH + "/python")
sys.path.insert(0, os.path.join(SPARK_HOME_PATH, "python/lib/" + PY4J_VERSION))
from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth

from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from analytics_platform.kronos.src.kronos_online_scoring import (
    load_user_eco_to_kronos_model_dict_s3,
    score_eco_user_package_dict)
from analytics_platform.kronos.softnet.src.softnet_constants import (
    MANIFEST_FILEPATH, MANIFEST_ECOSYSTEM, MANIFEST_PACKAGE_LIST)
from util.data_store.s3_data_store import S3DataStore
import analytics_platform.kronos.uranus.src.uranus_constants as test_const
from util.analytics_platform_util import get_path_names


class CompanionAccuracy(object):

    def __init__(self):
        self.user_eco_kronos_dict = load_user_eco_to_kronos_model_dict_s3(
            bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(
            bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.all_list_of_package_list = []
        self.test_set = set()
        self.search_set = set()

    def load_package_list(self, input_manifest_data_store, additional_path):
        """Generate the aggregated manifest list for a given ecosystem.

        :param input_manifest_data_store: The Data store to pick the manifest files from.
        :param additional_path: The directory to pick the manifest files from."""

        manifest_filenames = input_manifest_data_store.list_files(
            additional_path + MANIFEST_FILEPATH)
        for manifest_filename in manifest_filenames:
            user_category = manifest_filename.split("/")[-2]
            manifest_content_json_list = input_manifest_data_store.read_json_file(
                filename=manifest_filename)
            for manifest_content_json in manifest_content_json_list:
                manifest_content_dict = dict(manifest_content_json)
                ecosystem = manifest_content_dict.get(MANIFEST_ECOSYSTEM)
                if ecosystem != KRONOS_SCORING_REGION:
                    continue
                list_of_package_list = manifest_content_dict.get(
                    MANIFEST_PACKAGE_LIST)
                for each_stack_list in list_of_package_list:
                    lower_each_stack_list = set([package.lower()
                                                 for package in each_stack_list])
                    self.all_list_of_package_list.append(
                        list(lower_each_stack_list))

    def generate_freq_5_items(self):
        """Run Spark FP Growth to get the frequent item sets.

        :return: Only those frequent items sets where len(item_set)==5."""

        sc = SparkContext()
        rdd = sc.parallelize(self.all_list_of_package_list,
                             test_const.NUM_PARTITIONS)
        model = FPGrowth.train(
            rdd, test_const.MIN_SUPPORT_COUNT, test_const.NUM_PARTITIONS)
        freq_item_sets = model.freqItemsets().collect()
        freq_items_5 = []
        for item_set in freq_item_sets:
            item_set_len = len(item_set.items)
            if item_set_len == 5:
                freq_items_5.append(set(item_set.items))

        return freq_items_5

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
                "comp_package_count_threshold": test_const.COMPANION_COUNT_THRESHOLD,
                "alt_package_count_threshold": test_const.ALTERNATE_COUNT_THRESHOLD,
                "outlier_probability_threshold": test_const.OUTLIER_PROBABILITY_THRESHOLD,
                "unknown_packages_ratio_threshold": test_const. UNKNOWN_PROBABILITY_THRESHOLD,
                "package_list": package_list
            }
        ]

    def generate_test_set(self):
        """Generate the test data set using freq item sets."""

        freq_items_5 = self.generate_freq_5_items()
        for each_item_list in freq_items_5:
            combinations_4 = list(itertools.combinations(each_item_list, 4))
            for each_combination in combinations_4:
                self.test_set.add(frozenset(each_combination))

    def generate_whole_set(self):
        """Generate the set used for searching companion subsets."""

        for each_stack_list in self.all_list_of_package_list:
            self.search_set.add(frozenset(each_stack_list))

    def check_present(self, check_set):
        """Check if a given set is a subset of our manifest search set or not."""

        for each_set in self.search_set:
            if check_set.issubset(each_set):
                return True
        return False

    def companion_precision(self):
        """Score each test set and generate the recommendations.
        For each companion recommendation check its presence in the search set.
        If companion subset matches increase the true positive counter
            else the false positive counter."""

        t0 = time.time()
        counter = 1
        true_positives = 0
        false_positives = 0
        for each_test_set in self.test_set:
            input_list = list(each_test_set)
            result = self.predict_and_score(
                self.create_input_dict(input_list))
            comp_list = [com_pck.get('package_name') for com_pck in result[
                0].get('companion_packages', {})]
            for comp_pck in comp_list:
                temp_input = input_list[:]
                temp_input.append(comp_pck)
                if self.check_present(frozenset(temp_input)):
                    true_positives += 1
                else:
                    false_positives += 1
            print(counter)
            counter += 1

        print("For {} test_sets it took {} seconds to test.".format(
            counter - 1, time.time() - t0))
        print("Companion: True Positives = {}".format(true_positives))
        print("Companion: False Positives = {}".format(false_positives))
        print("Companion: Precision Percentage = {}".format(
            true_positives / (true_positives + false_positives) * 100))


if __name__ == '__main__':
    training_data_url = "s3://dev-stack-analysis-clean-data/maven/github/"
    input_bucket_name, _, additional_path = get_path_names(
        training_data_url)
    input_manifest_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                            access_key=AWS_S3_ACCESS_KEY_ID,
                                            secret_key=AWS_S3_SECRET_ACCESS_KEY)
    comp_acc_obj = CompanionAccuracy()
    comp_acc_obj.load_package_list(input_manifest_data_store, additional_path)
    comp_acc_obj.generate_test_set()
    comp_acc_obj.generate_whole_set()
    comp_acc_obj.companion_precision()
