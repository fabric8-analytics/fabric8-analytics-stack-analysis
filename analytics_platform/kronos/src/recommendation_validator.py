from collections import Counter
import json
from analytics_platform.kronos.src import config
from analytics_platform.kronos.softnet.src.softnet_constants import *
from util.data_store.s3_data_store import S3DataStore


class RecommendationValidator(object):

    def __init__(self, all_list_of_package_list):
        self.all_list_of_package_list = all_list_of_package_list
        self.manifest_len = len(self.all_list_of_package_list)

    @staticmethod
    def load_package_list(input_manifest_data_store, additional_path, input_ecosystem):
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
        input_manifest_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
        return cls.load_package_list(input_manifest_data_store, additional_path, input_ecosystem)

    def generate_companion_dependency_set(self, input_list, companion_package):
        if not None == input_list and not None == companion_package:
            input_list.append(companion_package)
            recommended_dependency_set = set(input_list)
            return recommended_dependency_set
        return set()

    def generate_alternate_dependency_set(self, input_list, alternate_package, alternate_to):
        if not None == input_list and not None == alternate_package and not None == alternate_to:
            if alternate_to in input_list:
                recommended_dependency_set = set(input_list)
                recommended_dependency_set.remove(alternate_to)
                recommended_dependency_set.add(alternate_package)
                return recommended_dependency_set
        return set()

    def check_alternate_recommendation_validity(self, recommended_dependency_set):
        count = 0
        for dependency_list in self.all_list_of_package_list:
            manifest_dependency_set = set(dependency_list)
            if manifest_dependency_set >= recommended_dependency_set:
                count += 1
        return count

    def check_companion_recommendation_validity(self, recommended_dependency_set):
        difference_list = []
        for dependency_list in self.all_list_of_package_list:
            manifest_dependency_set = set(dependency_list)
            if manifest_dependency_set >= recommended_dependency_set:
                diff = manifest_dependency_set - recommended_dependency_set
                difference_list.extend(diff)
        return difference_list

    def check_alternate_recommendation(self, input_list, alternate_packages):
        final_alternate = {}
        for each_recommendation in alternate_packages:
            final_each_recommendation = []
            alternate_to = each_recommendation
            for alternate_package in alternate_packages[each_recommendation]:
                alternate_package_name = alternate_package['package_name']
                recommended_dependency_set = self.generate_alternate_dependency_set(
                    input_list, alternate_package_name, alternate_to)
                count_value = self.check_alternate_recommendation_validity(
                    recommended_dependency_set)
                # if count_value:
                alternate_package["similarity_score"] = count_value
                final_each_recommendation.append(alternate_package)
            final_alternate[each_recommendation] = final_each_recommendation
        return final_alternate

    def check_companion_recommendation(self, input_list, companion):
        final_companion = []
        count = Counter()
        for each_recommendation in companion:
            companion_package = each_recommendation.get('package_name')
            recommended_dependency_set = self.generate_companion_dependency_set(
                input_list, companion_package)
            difference_list = self.check_companion_recommendation_validity(
                recommended_dependency_set)
            for package in difference_list:
                count[package] += 1
        for each_recommendation in companion:
            # comp_count = count[each_recommendation.get('package_name')]
            # if comp_count:
            each_recommendation["cooccurrence_probability"] = count[
                each_recommendation.get('package_name')]
            final_companion.append(each_recommendation)
        return final_companion

    def get_filtered_alternate_list(self, alternate_package, outlier_packages):
        only_outlier_alternate = {}
        if not None == alternate_package and not None == outlier_packages:
            outliers = [outlier['package_name'] for outlier in outlier_packages]
            for package in alternate_package:
                if package in outliers:
                    only_outlier_alternate[package] = alternate_package[package]
        return only_outlier_alternate

    def get_filtered_input_list(self, input_list, missing_packages):
        filtered_input_list = []
        if not None == input_list and not None == missing_packages:
            filtered_input_list = [
                package for package in input_list if package not in missing_packages]
        return filtered_input_list

    def get_all_list_package_list(self):
        return self.all_list_of_package_list

    def get_all_list_package_length(self):
        return self.manifest_len
