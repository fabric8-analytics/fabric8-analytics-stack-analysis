# NOTE: Works only with S3DataStore
import time

from analytics_platform.kronos.src.config import KRONOS_SCORING_REGION
from analytics_platform.kronos.uranus.src.uranus_constants import ALTERNATE_COUNT_THRESHOLD
from analytics_platform.kronos.uranus.src.super_class import Accuracy


class AlternateAccuracy(Accuracy):

    def __init__(self):
        super(AlternateAccuracy, self).__init__()
        self.freq_items_4 = []
        self.unique_package_dict = {}

    def generate_package_index(self):
        """For each unique package in the manifest generate the reverse index for the package."""

        for counter, each_stack in enumerate(self.freq_items_4):
            for each_package in each_stack:
                current_list = []
                if each_package in self.unique_package_dict:
                    current_list = self.unique_package_dict[each_package]
                current_list.append(counter)
                self.unique_package_dict[each_package] = current_list

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

            print(counter)
            counter += 1

        print("\n")
        print(("For {} test cases it took {} seconds to test.".format(
            counter - 1, time.time() - t0)))
        print(("Alternate: True Positives = {}".format(true_positives)))
        print(("Alternate: False Positives = {}".format(false_positives)))
        print(("Alternate: Precision Percentage = {}".format(
            true_positives / (true_positives + false_positives) * 100)))


if __name__ == '__main__':
    training_data_url = "s3://dev-stack-analysis-clean-data/maven/github/"
    alt_acc_obj = AlternateAccuracy()
    alt_acc_obj.load_package_list(training_data_url)
    alt_acc_obj.freq_items_4 = alt_acc_obj.generate_freq_items(4)
    alt_acc_obj.generate_whole_set()
    alt_acc_obj.generate_package_index()
    alt_acc_obj.alternate_precision()
