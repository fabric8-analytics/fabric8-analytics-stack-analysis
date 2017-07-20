from analytics_platform.kronos.pgm.src.pgm_constants import *
from analytics_platform.kronos.pgm.src.pgm_pomegranate import PGMPomegranate
from analytics_platform.kronos.pgm.src.pgm_util import *

from analytics_platform.kronos.src import config
from util.data_store.s3_data_store import S3DataStore


def load_user_eco_to_kronos_model_dict(input_kronos_data_store, additional_path):
    kronos_model_filenames = input_kronos_data_store.list_files(additional_path + KRONOS_OUTPUT_FOLDER)
    temp_user_eco_to_kronos_model_dict = dict()
    user_category_list = list()
    ecosystem_list = list()

    for kronos_model_filename in kronos_model_filenames:
        user_category = kronos_model_filename.split("/")[-2]
        if user_category not in user_category_list:
            user_category_list.append(user_category)
        ecosystem = kronos_model_filename.split("/")[-1].split(".")[0].split("_")[-1]
        if ecosystem not in ecosystem_list:
            ecosystem_list.append(ecosystem)
        kronos_model = PGMPomegranate.load(data_store=input_kronos_data_store,
                                           filename=kronos_model_filename)
        temp_user_eco_to_kronos_model_dict[
            (user_category, ecosystem)] = kronos_model

    user_eco_to_kronos_model_dict = dict()

    for user_category in user_category_list:
        eco_to_kronos_model_dict = dict()
        for ecosystem in ecosystem_list:
            eco_to_kronos_model_dict[ecosystem] = temp_user_eco_to_kronos_model_dict[
                (user_category, ecosystem)]
        user_eco_to_kronos_model_dict[user_category] = eco_to_kronos_model_dict

    return user_eco_to_kronos_model_dict


def load_user_eco_to_kronos_model_dict_s3(bucket_name, additional_path):
    input_data_store = S3DataStore(src_bucket_name=bucket_name,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    user_eco_to_kronos_model_dict = load_user_eco_to_kronos_model_dict(input_kronos_data_store=input_data_store,
                                                                       additional_path=additional_path)
    return user_eco_to_kronos_model_dict


def get_sorted_companion_package_probabilities(res, node_list, non_companion_packages):
    result = get_sorted_companion_node_probabilities(res, node_list, non_companion_packages)
    return result


def get_sorted_companion_intent_probabilities(res, node_list):
    result = get_sorted_companion_node_probabilities(res, node_list)
    return result


def get_sorted_companion_node_probabilities(res, node_list, non_companion_packages=None):
    result_dict_list = list()
    for i in range(0, len(node_list)):
        node = node_list[i]
        if non_companion_packages is not None:
            if node not in non_companion_packages:
                result_dict = dict()
                result_dict[KRONOS_COMPANION_PACKAGE_NAME] = node
                result_dict[KRONOS_COMPANION_PROBABILITY] = res[i].values()[1]
                result_dict_list.append(result_dict)
        else:
            result_dict = dict()
            result_dict[KRONOS_COMPANION_INTENT_NAME] = node
            result_dict[KRONOS_COMPANION_PROBABILITY] = res[i].values()[1]
            result_dict_list.append(result_dict)
    sorted_result_dict_list = sorted(result_dict_list, key=lambda x: x[KRONOS_COMPANION_PROBABILITY], reverse=True)
    return sorted_result_dict_list


def get_eco_to_kronos_dependency_dict(data_store, folderpath):
    eco_to_kronos_dependency_dict = dict()
    filenames = data_store.list_files(prefix=folderpath)
    for filename in filenames:
        ecosystem = filename.split("_")[-1].replace(".json", "")
        eco_to_kronos_dependency_dict[ecosystem] = dict(data_store.read_json_file(filename=filename))
    return eco_to_kronos_dependency_dict


def get_eco_to_cooccurrence_matrix_dict(data_store, folderpath):
    eco_to_cooccurrence_matrix_dict = dict()
    filenames = data_store.list_files(prefix=folderpath)
    for filename in filenames:
        ecosystem = filename.split("_")[-1].replace(".json", "")
        eco_to_cooccurrence_matrix_dict[ecosystem] = data_store.read_json_file_into_pandas_df(filename=filename)
    return eco_to_cooccurrence_matrix_dict


def get_companion_package_dict(result, package_list, non_companion_packages):
    package_result_dict = get_sorted_companion_package_probabilities(result, package_list, non_companion_packages)
    return package_result_dict


def generated_evidence_dict_list_and_potential_outlier_index_list(observed_package_list, node_list):
    evidence_dict_list = list()
    potential_outlier_index_list = list()
    for i in range(len(observed_package_list)):
        temp_list = list(observed_package_list)
        node = observed_package_list[i]
        potential_outlier_index_list.append(node_list.index(node))
        temp_list.pop(i)
        temp_evidence_list = generate_evidence_map_from_transaction_list(temp_list)
        evidence_dict_list.append(temp_evidence_list)
    evidence_dict_list.append(generate_evidence_map_from_transaction_list(observed_package_list))
    return evidence_dict_list, potential_outlier_index_list


def get_kronos_recommendation(kronos, observed_package_list, node_list, outlier_threshold):
    evidence_dict_list, potential_outlier_index_list = generated_evidence_dict_list_and_potential_outlier_index_list(
        observed_package_list, node_list)

    result_array = kronos.score(evidence_dict_list=evidence_dict_list)
    outlier_dict_list = list()
    companion_recommendation_dict = result_array[-1]

    for i in range(0, len(potential_outlier_index_list)):
        outlier_result = result_array[i][potential_outlier_index_list[i]]
        outlier_score = outlier_result.values()[0]
        if outlier_score > outlier_threshold:
            outlier_dict = dict()
            outlier_dict[KRONOS_OUTLIER_PACKAGE_NAME] = observed_package_list[i]
            outlier_dict[KRONOS_OUTLIER_PROBABILITY] = outlier_score
            outlier_dict_list.append(outlier_dict)
    sorted_outlier_dict_list = sorted(outlier_dict_list, key=lambda x: x[KRONOS_OUTLIER_PROBABILITY], reverse=True)
    return companion_recommendation_dict, sorted_outlier_dict_list


def get_non_companion_packages(alternate_package_dict, observed_package_list):
    alternate_package_list = list()
    for package in alternate_package_dict:
        alternate_package_list_of_package = [item.get(KD_PACKAGE_NAME) for item in alternate_package_dict[package]]
        alternate_package_list += alternate_package_list_of_package

    non_companion_packages = alternate_package_list + observed_package_list
    return non_companion_packages


def get_observed_and_missing_package_list(requested_package_list, unknown_package_ratio_threshold, package_list):
    observed_package_list = None
    existing_package_set = set(requested_package_list).intersection(package_list)
    missing_package_set = set(requested_package_list) - existing_package_set
    missing_package_list = list(missing_package_set)
    acceptable_existing_package_count = (1 - unknown_package_ratio_threshold) * len(requested_package_list)
    if acceptable_existing_package_count <= len(existing_package_set):
        observed_package_list = list(existing_package_set)
    return observed_package_list, missing_package_list


def score_kronos(kronos, requested_package_list, kronos_dependency, comp_package_count_threshold,
                 alt_package_count_threshold, outlier_probability_threshold, unknown_package_ratio_threshold):
    package_list = kronos_dependency.get(KD_PACKAGE_LIST)
    intent_list = kronos_dependency.get(KD_INTENT_LIST)
    node_list = package_list + intent_list

    observed_package_list, missing_package_list = get_observed_and_missing_package_list(requested_package_list,
                                                                                        unknown_package_ratio_threshold,
                                                                                        package_list)

    alternate_package_dict = {}
    companion_package_dict_list_pruned = list()
    outlier_package_dict_list = list()

    if observed_package_list is not None:
        similar_package_json = kronos_dependency.get(KD_SIMILAR_PACKAGE_MAP)
        similar_package_dict = dict(similar_package_json)
        alternate_package_dict = get_alternate_packages_for_packages(similar_package_dict=similar_package_dict,
                                                                     package_names=observed_package_list,
                                                                     alt_package_count_threshold=alt_package_count_threshold)

        non_companion_packages = get_non_companion_packages(alternate_package_dict, observed_package_list)

        companion_recommendation_dict, outlier_package_dict_list = get_kronos_recommendation(kronos,
                                                                                             observed_package_list,
                                                                                             node_list,
                                                                                             outlier_probability_threshold)

        companion_package_dict_list = get_companion_package_dict(result=companion_recommendation_dict,
                                                                 package_list=package_list,
                                                                 non_companion_packages=non_companion_packages)
        companion_package_dict_list_pruned = companion_package_dict_list[0:comp_package_count_threshold]

    result = dict()
    result[KRONOS_ALTERNATE_PACKAGES] = alternate_package_dict
    result[KRONOS_COMPANION_PACKAGES] = companion_package_dict_list_pruned
    result[KRONOS_OUTLIER_PACKAGES] = outlier_package_dict_list
    result[KRONOS_MISSING_PACKAGES] = missing_package_list
    return result


def get_alternate_packages_for_packages(similar_package_dict, package_names, alt_package_count_threshold):
    alternate_package_dict = dict()
    for package_name in package_names:
        alternate_package_dict_list_of_package = similar_package_dict[package_name]
        num_alternate_packages = len(alternate_package_dict_list_of_package)
        if num_alternate_packages > alt_package_count_threshold:
            num_alternate_packages = alt_package_count_threshold
        alternate_package_dict_list_of_package_pruned = [alternate_package_dict_list_of_package[i] for i in
                                                         range(num_alternate_packages)]
        alternate_package_dict[package_name] = alternate_package_dict_list_of_package_pruned
    return alternate_package_dict


def score_eco_user_package_dict(user_request, user_eco_kronos_dict, eco_to_kronos_dependency_dict):
    comp_package_count_threshold = KRONOS_COMPANION_PACKAGE_COUNT_THRESHOLD_VALUE
    alt_package_count_threshold = KRONOS_ALTERNATE_PACKAGE_COUNT_THRESHOLD_VALUE
    outlier_probability_threshold = KRONOS_OUTLIER_PROBABILITY_THRESHOLD_VALUE
    unknown_package_ratio_threshold = KRONOS_UNKNOWN_PACKAGE_RATIO_THRESHOLD_VALUE

    request_json_list = list(user_request)

    response_json_list = list()
    for request_json in request_json_list:
        request_dict = dict(request_json)
        ecosystem = request_dict[KRONOS_SCORE_ECOSYSTEM]
        if KRONOS_SCORE_USER_PERSONA in request_dict:
            user_category = request_dict[KRONOS_SCORE_USER_PERSONA]
        else:
            user_category = "1"
        if KRONOS_COMPANION_PACKAGE_COUNT_THRESHOLD_NAME in request_dict:
            comp_package_count_threshold = request_dict[KRONOS_COMPANION_PACKAGE_COUNT_THRESHOLD_NAME]
        if KRONOS_ALTERNATE_PACKAGE_COUNT_THRESHOLD_NAME in request_dict:
            alt_package_count_threshold = request_dict[KRONOS_ALTERNATE_PACKAGE_COUNT_THRESHOLD_NAME]
        if KRONOS_OUTLIER_PROBABILITY_THRESHOLD_NAME in request_dict:
            outlier_probability_threshold = request_dict[KRONOS_OUTLIER_PROBABILITY_THRESHOLD_NAME]
        if KRONOS_UNKNOWN_PACKAGE_RATIO_THRESHOLD_NAME in request_dict:
            unknown_package_ratio_threshold = request_dict[KRONOS_UNKNOWN_PACKAGE_RATIO_THRESHOLD_NAME]

        requested_package_list = request_dict[KRONOS_SCORE_PACKAGE_LIST]
        package_list_lower_case = [x.lower() for x in requested_package_list]
        kronos = user_eco_kronos_dict[user_category][ecosystem]
        kronos_dependency = eco_to_kronos_dependency_dict[ecosystem]
        prediction_result_dict = score_kronos(kronos=kronos,
                                              requested_package_list=package_list_lower_case,
                                              kronos_dependency=kronos_dependency,
                                              comp_package_count_threshold=comp_package_count_threshold,
                                              alt_package_count_threshold=alt_package_count_threshold,
                                              outlier_probability_threshold=outlier_probability_threshold,
                                              unknown_package_ratio_threshold=unknown_package_ratio_threshold)
        prediction_result_dict[KRONOS_SCORE_USER_PERSONA] = user_category
        prediction_result_dict[KRONOS_SCORE_ECOSYSTEM] = ecosystem

        response_json_list.append(prediction_result_dict)
    return response_json_list
