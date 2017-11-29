# NOTE: Works only with S3DataStore
from __future__ import division
from analytics_platform.kronos.src.config import (
    AWS_BUCKET_NAME,
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY,
    KRONOS_MODEL_PATH,
    KRONOS_SCORING_REGION)
import time
from analytics_platform.kronos.pgm.src.offline_training import load_eco_to_kronos_dependency_dict_s3
from analytics_platform.kronos.softnet.src.softnet_constants import (
    MANIFEST_FILEPATH, MANIFEST_ECOSYSTEM, MANIFEST_PACKAGE_LIST)
from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names


class AlternateAccuracy(object):

    def __init__(self):
        self.eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict_s3(
            bucket_name=AWS_BUCKET_NAME, additional_path=KRONOS_MODEL_PATH)
        self.all_list_of_package_list = []
        self.unique_package_dict = {}

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
                    lower_each_stack = frozenset([package.lower()
                                                  for package in each_stack_list])
                    self.all_list_of_package_list.append(
                        lower_each_stack)

    def generate_package_index(self):
        """For each unique package in the manifest generate the reverse index for the package."""

        for counter, each_stack in enumerate(self.all_list_of_package_list):
            for each_package in each_stack:
                current_list = []
                if each_package in self.unique_package_dict:
                    current_list = self.unique_package_dict[each_package]
                current_list.append(counter)
                self.unique_package_dict[each_package] = current_list

    def check_present(self, check_set):
        """Check if a given set is a subset of our manifest search set or not."""

        for each_set in self.all_list_of_package_list:
            if check_set.issubset(each_set):
                return True
        return False

    def generate_alternate_dependency_set(self, input_list, alternate_package, alternate_to):
        """Replace the  alternate package in the current input list to generate a new test stack.

        :param input_list: The original package list of the user stack.
        :param alternate_package: The PGM recommended alternate package.
        :param alternate_to: The user package for which alternate is recommended.

        :return: A set() of user_stack + alternate_package - alternate_to."""
        if alternate_package is None or alternate_to is None or len(input_list) == 0:
            return frozenset()
        return frozenset([alternate_package if package == alternate_to else package
                          for package in input_list])

    def alternate_precision(self):
        """Test all similarity packages.
        For each alternate recommendation check its presence in the original manifest list.
        If alternate subset matches increase the true positive counter
            else the false positive counter."""

        t0 = time.time()
        counter = 1
        true_positives = 0
        false_positives = 0
        data = self.eco_to_kronos_dependency_dict[
            KRONOS_SCORING_REGION].get('similar_package_dict', [])
        for each_package in data:
            possible_alternates = data.get(each_package, [])
            for each_alternate in possible_alternates:
                alternate_package = each_alternate.get('package_name')
                for each_index in self.unique_package_dict.get(each_package, []):
                    check_set = self.generate_alternate_dependency_set(
                        self.all_list_of_package_list[each_index], alternate_package, each_package)
                    if self.check_present(check_set):
                        true_positives += 1
                        break
                else:
                    false_positives += 1

            print(counter)
            counter += 1

        print("For {} test_sets it took {} seconds to test.".format(
            counter - 1, time.time() - t0))
        print("Alternate: True Positives = {}".format(true_positives))
        print("Alternate: False Positives = {}".format(false_positives))
        print("Alternate: Precision Percentage = {}".format(
            true_positives / (true_positives + false_positives) * 100))


if __name__ == '__main__':
    training_data_url = "s3://dev-stack-analysis-clean-data/maven/github/"
    input_bucket_name, _, additional_path = get_path_names(
        training_data_url)
    input_manifest_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                            access_key=AWS_S3_ACCESS_KEY_ID,
                                            secret_key=AWS_S3_SECRET_ACCESS_KEY)
    alt_acc_obj = AlternateAccuracy()
    alt_acc_obj.load_package_list(input_manifest_data_store, additional_path)
    alt_acc_obj.generate_package_index()
    alt_acc_obj.alternate_precision()
