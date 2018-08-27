"""Script to generate test data."""

from analytics_platform.kronos.src.config import (
    SPARK_HOME_PATH,
    PY4J_VERSION,
    KRONOS_SCORING_REGION)

from itertools import combinations
import os
import pickle
import sys
sys.path.insert(0, SPARK_HOME_PATH + "/python")
sys.path.insert(0, os.path.join(SPARK_HOME_PATH, "python/lib/" + PY4J_VERSION))
from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth

from evaluation_platform.uranus.src.uranus_constants import (
    NUM_PARTITIONS,
    MIN_SUPPORT_COUNT,
    URANUS_INPUT_RAW_PATH,
    URANUS_ECOSYSTEM,
    URANUS_PACKAGE_LIST,
    URANUS_OUTPUT_PATH)


class TestData(object):
    """Script to generate test data."""

    def __init__(self):
        """Initialize this class."""
        self.all_list_of_package_list = []
        self.search_set = set()
        self.freq_items_4 = []
        self.freq_items_5 = []
        self.unique_package_dict = {}
        self.comp_test_set = set()

    def load_package_list(self, input_data_store, additional_path):
        """Generate the aggregated manifest list for a given ecosystem.

        :param input_data_store: The Data store to pick the manifest files from.
        :param additional_path: The directory to pick the manifest files from.
        """
        manifest_filenames = input_data_store.list_files(
            os.path.join(additional_path,
                         URANUS_INPUT_RAW_PATH))
        for manifest_filename in manifest_filenames:
            user_category = manifest_filename.split("/")[-2]
            manifest_content_json_list = input_data_store.read_json_file(
                filename=manifest_filename)
            for manifest_content_json in manifest_content_json_list:
                manifest_content_dict = dict(manifest_content_json)
                ecosystem = manifest_content_dict.get(URANUS_ECOSYSTEM)
                if ecosystem != KRONOS_SCORING_REGION:
                    continue
                list_of_package_list = manifest_content_dict.get(
                    URANUS_PACKAGE_LIST)
                for each_stack_list in list_of_package_list:
                    lower_each_stack_list = set([package.lower()
                                                 for package in each_stack_list])
                    self.all_list_of_package_list.append(
                        list(lower_each_stack_list))

    def generate_freq_items(self):
        """Run Spark FP Growth to get the frequent item sets.

        :return: Only those frequent items sets where len(item_set) is either 4 or 5
        """
        sc = SparkContext.getOrCreate()
        rdd = sc.parallelize(self.all_list_of_package_list,
                             NUM_PARTITIONS)
        model = FPGrowth.train(
            rdd, MIN_SUPPORT_COUNT, NUM_PARTITIONS)
        freq_item_sets = model.freqItemsets().collect()
        for item_set in freq_item_sets:
            item_set_len = len(item_set.items)
            if item_set_len == 4:
                self.freq_items_4.append(item_set.items)
            elif item_set_len == 5:
                self.freq_items_5.append(item_set.items)
        sc.stop()

    def generate_whole_set(self):
        """Generate the set used for searching companion subsets."""
        for each_stack_list in self.all_list_of_package_list:
            self.search_set.add(frozenset(each_stack_list))

    def generate_package_index(self):
        """For each unique package in the manifest generate its reverse index."""
        for counter, each_stack in enumerate(self.freq_items_4):
            for each_package in each_stack:
                current_list = []
                if each_package in self.unique_package_dict:
                    current_list = self.unique_package_dict[each_package]
                current_list.append(counter)
                self.unique_package_dict[each_package] = current_list

    def generate_comp_test_set(self):
        """Generate the test data set using freq item sets."""
        for each_item_list in self.freq_items_5:
            combinations_4 = list(combinations(each_item_list, 4))
            for each_combination in combinations_4:
                self.comp_test_set.add(frozenset(each_combination))

    @staticmethod
    def save_json_file(output_data_store, additional_path, filename, contents):
        """Save the contents as a JSON file at the given datastore.

        :param output_data_store: The datastore where json will be saved.
        :param additional_path: The directory where the json will be saved.
        :param filename: The name under which json data will be saved.
        :param contents: The json data to be saved.
        """
        output_filename = os.path.join(
            additional_path, URANUS_OUTPUT_PATH, filename)
        output_data_store.write_json_file(output_filename, contents)

    @staticmethod
    def save_pickle_file(output_data_store, additional_path, pickle_filename):
        """Save the pickle file at the given datastore.

        :param output_data_store: The datastore where json will be saved.
        :param additional_path: The directory where the json will be saved.
        :param pickle_filename: The locally stored pickle file to be saved.
        """
        complete_filename = os.path.join(
            additional_path, URANUS_OUTPUT_PATH, pickle_filename)
        output_data_store.write_pickle_file(
            complete_filename=complete_filename,
            pickle_filename=pickle_filename)

    @staticmethod
    def pickle_dump(pickle_filename, contents):
        """Save the contents as a pickle file.

        :param  contents: The data to pickle
        :param pickle_filename: The file name under which data is pickled.
        """
        with open(os.path.join('/tmp', pickle_filename), 'wb') as handle:
            # IMPORTANT: Set pickle.HIGHEST_PROTOCOL only  after complete porting to
            # Python3
            pickle.dump(contents, handle,
                        protocol=2)

    def generate_attributes(self, input_data_store, additional_path):
        """Generate all the required attributes of the class object.

        :param input_data_store: The datastore where the raw data is present.
        :param additional_path: The directory where raw data is picked from.
        """
        self.load_package_list(input_data_store, additional_path)
        self.generate_freq_items()
        self.generate_whole_set()
        self.generate_comp_test_set()
        self.generate_package_index()

    def save_attributes(self, output_data_store, additional_path):
        """Save all the required attributes of the class object.

        :param output_data_store: The datastore where the test data is to be put.
        :param additional_path: The directory where test data is stored.
        """
        self.save_json_file(
            output_data_store,
            additional_path,
            "reverse_dict.json",
            self.unique_package_dict)
        self.save_json_file(
            output_data_store,
            additional_path,
            "freq_4.json",
            self.freq_items_4)
        self.pickle_dump(
            "search_set.pickle",
            self.search_set)
        self.save_pickle_file(
            output_data_store,
            additional_path,
            "search_set.pickle")
        self.pickle_dump(
            "comp_test_set.pickle",
            self.comp_test_set)
        self.save_pickle_file(
            output_data_store,
            additional_path,
            "comp_test_set.pickle")
