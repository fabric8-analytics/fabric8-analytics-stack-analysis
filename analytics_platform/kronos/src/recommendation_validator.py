from collections import Counter
import json
from analytics_platform.kronos.src import config
from analytics_platform.kronos.softnet.src.softnet_constants import MANIFEST_FILEPATH, MANIFEST_ECOSYSTEM, MANIFEST_PACKAGE_LIST
from util.data_store.s3_data_store import S3DataStore


class RecommendationValidator(object):
    """
    Recommendation Validator validates the recommendations sent by the PGM model.
    """

    def __init__(self, all_list_of_package_list):
        """
        Instantiate Recommendation Validator
        """

        self.all_list_of_package_list = all_list_of_package_list
        self.manifest_len = len(self.all_list_of_package_list)

    @staticmethod
    def load_package_list(input_manifest_data_store, additional_path, input_ecosystem):
        """
        Generate the aggregated manifest list for a given ecosystem.

        :param input_manifest_data_store: The Data store to pick the manifest files from.
        :param additional_path: The directory to pick the manifest files from.
        :param input_ecosystem: The ecosystem for which the aggregated manifest list will be saved.

        :return: RecommendationValidator object
        """

        all_list_of_package_list = []
        manifest_filenames = input_manifest_data_store.list_files(
            additional_path + MANIFEST_FILEPATH)
        for manifest_filename in manifest_filenames:
            user_category = manifest_filename.split("/")[-2]
            manifest_content_json_list = input_manifest_data_store.read_json_file(
                filename=manifest_filename)
            for manifest_content_json in manifest_content_json_list:
                manifest_content_dict = dict(manifest_content_json)
                ecosystem = manifest_content_dict[MANIFEST_ECOSYSTEM]
                if ecosystem != input_ecosystem:
                    continue
                list_of_package_list = manifest_content_dict.get(
                    MANIFEST_PACKAGE_LIST)
                all_list_of_package_list.extend(list_of_package_list)
        return RecommendationValidator(all_list_of_package_list)

    @classmethod
    def load_package_list_s3(cls, input_bucket_name, additional_path, input_ecosystem):
        """
        Generate the aggregated manifest list for a given ecosystem for S3 datasource

        :param input_bucket_name: The bucket where the manifest files are stored.
        :param additional_path: The directory to pick the manifest files from.
        :param input_ecosystem: The ecosystem for which the aggregated manifest list will be saved.

        :return: RecommendationValidator object
        """

        # Create a S3 object
        input_manifest_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
        return cls.load_package_list(input_manifest_data_store, additional_path, input_ecosystem)

    def generate_companion_dependency_set(self, input_list, companion_package):
        """
        Append companion package to the current input list to generate a new test stack.

        :param input_list: The original package list of the user stack.
        :param companion_package: The PGM recommended companion package.

        :return: A set() of user_stack + companion_package
        """

        if not None == input_list and not None == companion_package:
            input_list = input_list[:]
            input_list.append(companion_package)
            recommended_dependency_set = set(input_list)
            return recommended_dependency_set
        return set()

    def generate_alternate_dependency_set(self, input_list, alternate_package, alternate_to):
        """
        Replace the  alternate package in the current input list to generate a new test stack.

        :param input_list: The original package list of the user stack.
        :param alternate_package: The PGM recommended alternate package.
        :param alternate_to: The user package for which alternate is recommended.

        :return: A set() of user_stack + alternate_package - alternate_to
        """

        if not None == input_list and not None == alternate_package and not None == alternate_to:
            input_list = input_list[:]
            if alternate_to in input_list:
                recommended_dependency_set = set(input_list)
                recommended_dependency_set.remove(alternate_to)
                recommended_dependency_set.add(alternate_package)
                return recommended_dependency_set
        return set()

    def check_alternate_recommendation_validity(self, recommended_dependency_set):
        """
        Check if the given test stack has a valid alternate recommendation.
        An alternate recommendation is valid if the test stack is subset of any dependency set in the aggregated manifest list.

        :param recommended_dependency_set: The test stack obtained from fnc. generate_alternate_dependency_set()

        :return: Frequency of subset where test stack is a part of any known dependency list.
        """

        count = 0
        for dependency_list in self.all_list_of_package_list:
            manifest_dependency_set = set(dependency_list)
            if manifest_dependency_set >= recommended_dependency_set:
                count += 1
        return count

    def check_companion_recommendation_validity(self, recommended_dependency_set, input_list):
        """
        Check if the given test stack has a valid companion recommendation.
        A companion recommendation is valid if the test stack is subset of any dependency set in the aggregated manifest list.

        :param recommended_dependency_set: The test stack obtained from fnc. generate_companion_dependency_set()

        :return: The list of companion packages observed among the known dependency set.
        """
        input_list = input_list[:]
        input_set = set(input_list)
        difference_list = []
        for dependency_list in self.all_list_of_package_list:
            diff = []
            manifest_dependency_set = set(dependency_list)
            if manifest_dependency_set > recommended_dependency_set:
                # diff gives us list of packges present in the known stack vs
                # the input stack, leaving only the companion packages
                diff = list(manifest_dependency_set -
                            recommended_dependency_set)
            elif manifest_dependency_set == recommended_dependency_set:
                diff = list(recommended_dependency_set - input_set)
            difference_list.extend(diff)
        return difference_list

    def check_alternate_recommendation(self, input_list, alternate_packages):
        """
        Return the filtered alternate recommendations after validation.

        :param input_list: The original package list of the user stack.
        :param alternate_packages: The alternate package list recommended by PGM.

        :return: The list of valid alternate package recommendations.
        """

        # TODO: Validate alternate packages for all combinations of replacement.

        final_alternate_recommendations = {}
        # alternate_packages is a dict of alternate recommendations.
        for each_recommendation in alternate_packages:
            temp_each_recommendation = []
            alternate_to = each_recommendation  # The package to be replaced
            # for each alternative of a package, do the validation test.
            for alternate_package in alternate_packages[each_recommendation]:
                alternate_package_name = alternate_package[
                    'package_name']  # The replacement
                recommended_dependency_set = self.generate_alternate_dependency_set(
                    input_list, alternate_package_name, alternate_to)
                count_value = self.check_alternate_recommendation_validity(
                    recommended_dependency_set)
                if count_value:
                    # change the value of similarity_score to reflect the actual manifest
                    # usage count.
                    alternate_package["similarity_score"] = count_value
                    temp_each_recommendation.append(alternate_package)
            final_alternate_recommendations[
                each_recommendation] = temp_each_recommendation
        return final_alternate_recommendations

    def check_companion_recommendation(self, input_list, companion_packages):
        """
        Return the filtered companion recommendations after validation.

        :param input_list: The original package list of the user stack.
        :param companion_packages: The companion package list recommended by PGM.

        :return: The list of valid companion package recommendations.
        """

        # TODO: Handle the possibility of a package present in Counter list but not
        # in PGM companion package list

        final_companion_recommendations = []
        count = Counter()
        for each_recommendation in companion_packages:
            companion_package = each_recommendation.get('package_name')
            recommended_dependency_set = self.generate_companion_dependency_set(
                input_list, companion_package)
            difference_list = self.check_companion_recommendation_validity(
                recommended_dependency_set, input_list)
            # Increase the frequency count each time a compaion package is found to be
            # a valid subset.
            for package in difference_list:
                count[package] += 1
        for each_recommendation in companion_packages:
            # TODO: Enable this for zero level filtering
            comp_count = count[each_recommendation.get('package_name')]
            if comp_count:
                # change the value of cooccurrence_probability to reflect the actual manifest
                # usage count.
                each_recommendation["cooccurrence_probability"] = comp_count
                final_companion_recommendations.append(each_recommendation)
        return final_companion_recommendations

    def get_filtered_alternate_list(self, alternate_package, outlier_packages):
        """
        Prune the PGM recommended alternate package list to include only the outlier packages for testing recommendation validation.

        :param alternate_package: The alternate package list recommended by PGM.
        :param outlier_packages: The list outliers in user stack recognised by PGM.

        :return : Filtered alternate package list
        """

        only_outlier_alternate = {}
        if not None == alternate_package and not None == outlier_packages:
            outliers = [outlier['package_name'] for outlier in outlier_packages]
            for package in alternate_package:
                if package in outliers:
                    only_outlier_alternate[package] = alternate_package[package]
        return only_outlier_alternate

    def get_filtered_input_list(self, input_list, missing_packages):
        """
        Prune the original package list to exclude the missing packages for testing recommendation validation.

        :param input_list: The original package list of the user stack.
        :param missing_packages: The list of Packages unknown to PGM.

        :return : Filtered input package list.
        """

        filtered_input_list = []
        if not None == input_list and not None == missing_packages:
            filtered_input_list = [
                package for package in input_list if package not in missing_packages]
        return filtered_input_list

    def get_all_list_package_list(self):
        return self.all_list_of_package_list

    def get_all_list_package_length(self):
        return self.manifest_len
