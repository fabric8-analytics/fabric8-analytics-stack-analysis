"""Test all similarity packages."""

import time
import os

from analytics_platform.kronos.src.config import KRONOS_SCORING_REGION
from evaluation_platform.uranus.src.uranus_constants import (
    ALTERNATE_COUNT_THRESHOLD,
    URANUS_OUTPUT_PATH)
from evaluation_platform.uranus.src.super_class import Accuracy

import daiquiri
import logging

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


class AlternateAccuracy(Accuracy):
    """Test all similarity packages."""

    def __init__(self):
        """Initialize this class."""
        super(AlternateAccuracy, self).__init__()
        self.freq_items_4 = []
        self.unique_package_dict = {}
        self.test_set_len = 0

    @staticmethod
    def load(input_data_store, filename):
        """Load a file from within a datastore.

        :param input_data_store: The datastore from where file is picked.
        :param filename: The file to be loaded.
        """
        return input_data_store.read_json_file(
            filename)

    def load_attributes(self, input_data_store, additional_path):
        """Load the required attributes of the class object.

        :param input_data_store: The datastore where test data is present.
        :param additional_path: The directory where test data is loaded from.
        """
        input_filename = os.path.join(
            additional_path, URANUS_OUTPUT_PATH, "reverse_dict.json")
        self.unique_package_dict = self.load(
            input_data_store,
            input_filename)
        self.test_set_len = len(self.unique_package_dict)

        input_filename = os.path.join(
            additional_path, URANUS_OUTPUT_PATH, "freq_4.json")
        self.freq_items_4 = self.load(
            input_data_store,
            input_filename)

        self.load_search_set(
            input_data_store,
            additional_path)

    def generate_alternate_dependency_set(self, input_list, alternate_package, alternate_to):
        """Replace the alternate package in the current input list to generate a new test stack.

        :param input_list: The original package list of the user stack.
        :param alternate_package: The PGM recommended alternate package.
        :param alternate_to: The user package for which alternate is recommended.

        :return: A set() of user_stack + alternate_package - alternate_to.
        """
        if alternate_package is None or alternate_to is None or len(input_list) == 0:
            return frozenset()
        return frozenset([alternate_package if package == alternate_to else package
                          for package in input_list])

    def alternate_precision(self):
        """Test all similarity packages.

        For each alternate recommendation check its presence in the original manifest list.
        If alternate subset matches increase the true positive counter
            else the false positive counter.

        :return alternate_precision_result: The evaluation result for alternate packages.
        """
        alternate_precision_result = {
            "Number of Test Cases": self.test_set_len}
        t0 = time.time()
        true_positives = 0.
        false_positives = 0.
        similarity_data = self.eco_to_kronos_dependency_dict[
            KRONOS_SCORING_REGION].get('similar_package_dict', [])
        for each_package in self.unique_package_dict:
            possible_alternates = similarity_data.get(each_package, [])
            for each_alternate in possible_alternates[:ALTERNATE_COUNT_THRESHOLD]:
                alternate_package = each_alternate.get('package_name')
                for each_index in self.unique_package_dict.get(each_package):
                    check_set = self.generate_alternate_dependency_set(
                        self.freq_items_4[each_index], alternate_package, each_package)
                    if self.check_present(check_set):
                        true_positives += 1
                        break
                else:
                    false_positives += 1

        alternate_precision_result["Time taken(sec)"] = time.time() - t0
        alternate_precision_result["True Positives"] = true_positives
        alternate_precision_result["False Positives"] = false_positives
        alternate_precision_result["Precision Percentage"] = true_positives / \
            (true_positives + false_positives) * 100

        _logger.info("Alternate testing ended in {} seconds".format(
            alternate_precision_result["Time taken(sec)"]))
        _logger.info("Alternate precision is {} %".format(
            alternate_precision_result["Precision Percentage"]))

        return alternate_precision_result
