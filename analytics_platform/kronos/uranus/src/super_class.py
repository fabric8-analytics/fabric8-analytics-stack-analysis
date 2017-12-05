# NOTE: Works only with S3DataStore
import time

from analytics_platform.kronos.src.config import (
    AWS_BUCKET_NAME,
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY,
    KRONOS_MODEL_PATH)
from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names
from analytics_platform.kronos.uranus.src.uranus_constants import URANUS_OUTPUT_PATH


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

        pickle_filename = "search_set.pickle"
        input_filename = additional_path + \
            URANUS_OUTPUT_PATH + pickle_filename
        self.search_set = input_data_store.load_pickle_file(
            filename=input_filename)

    @staticmethod
    def get_input_data_store(training_url):
        """Returns the input datastore and the additional path from a given URL.

        :param training_url: The URL where test data is read from and written into."""

        input_bucket_name, _, additional_path = get_path_names(
            training_url)
        input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                       access_key=AWS_S3_ACCESS_KEY_ID,
                                       secret_key=AWS_S3_SECRET_ACCESS_KEY)
        return input_data_store, additional_path
