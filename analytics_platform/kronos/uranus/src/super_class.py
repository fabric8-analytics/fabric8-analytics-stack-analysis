# NOTE: Works only with S3DataStore
from analytics_platform.kronos.src.config import (
    SPARK_HOME_PATH,
    PY4J_VERSION,
    AWS_BUCKET_NAME,
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY,
    KRONOS_MODEL_PATH,
    KRONOS_SCORING_REGION)

import os
import time
import sys
sys.path.insert(0, SPARK_HOME_PATH + "/python")
sys.path.insert(0, os.path.join(SPARK_HOME_PATH, "python/lib/" + PY4J_VERSION))
from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth

from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from analytics_platform.kronos.softnet.src.softnet_constants import (
    MANIFEST_FILEPATH, MANIFEST_ECOSYSTEM, MANIFEST_PACKAGE_LIST)
from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names
from analytics_platform.kronos.uranus.src.uranus_constants import (
    NUM_PARTITIONS,
    MIN_SUPPORT_COUNT)


class Accuracy(object):

    def __init__(self):
        self.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(
            bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.all_list_of_package_list = []
        self.search_set = set()

    def load_package_list(self, training_url):
        """Generate the aggregated manifest list for a given ecosystem.

        :param input_manifest_data_store: The Data store to pick the manifest files from.
        :param additional_path: The directory to pick the manifest files from."""

        input_manifest_data_store, additional_path = self.get_input_data_store(
            training_url)
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

    def generate_freq_items(self, freq_len):
        """Run Spark FP Growth to get the frequent item sets.

        :return: Only those frequent items sets where len(item_set)==freq_len."""

        freq_items = []
        sc = SparkContext()
        rdd = sc.parallelize(self.all_list_of_package_list,
                             NUM_PARTITIONS)
        model = FPGrowth.train(
            rdd, MIN_SUPPORT_COUNT, NUM_PARTITIONS)
        freq_item_sets = model.freqItemsets().collect()
        for item_set in freq_item_sets:
            item_set_len = len(item_set.items)
            if item_set_len == freq_len:
                freq_items.append(item_set.items)
        return freq_items

    def check_present(self, check_set):
        """Check if a given set is a subset of our manifest search set or not."""

        for each_set in self.search_set:
            if check_set.issubset(each_set):
                return True
        return False

    def generate_whole_set(self):
        """Generate the set used for searching companion subsets."""

        for each_stack_list in self.all_list_of_package_list:
            self.search_set.add(frozenset(each_stack_list))

    def get_input_data_store(self, training_url):
        input_bucket_name, _, additional_path = get_path_names(
            training_url)
        input_manifest_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                access_key=AWS_S3_ACCESS_KEY_ID,
                                                secret_key=AWS_S3_SECRET_ACCESS_KEY)
        return input_manifest_data_store, additional_path
