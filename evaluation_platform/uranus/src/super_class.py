# NOTE: Currenlty works only with S3DataStore
import time
import os

from analytics_platform.kronos.src.config import (
    AWS_BUCKET_NAME,
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY,
    KRONOS_MODEL_PATH)
from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names
from evaluation_platform.uranus.src.uranus_constants import URANUS_OUTPUT_PATH


class Accuracy(object):

    def __init__(self):
        self.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(
            bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.search_set = set()

    def check_present(self, check_set):
        """Check if a given set is a subset of our manifest search set or not.

        :param check_set: The set to checked for within the manifest set."""

        for each_set in self.search_set:
            if check_set.issubset(each_set):
                return True
        return False

    def load_search_set(self, input_data_store, additional_path):
        """Load the manifets search set from the given source.

        :param input_data_store: The datastore where search set is present.
        :param additional_path: The directory where search set is loaded from."""

        input_filename = os.path.join(
            additional_path, URANUS_OUTPUT_PATH, "search_set.pickle")
        self.search_set = input_data_store.load_pickle_file(
            filename=input_filename)
