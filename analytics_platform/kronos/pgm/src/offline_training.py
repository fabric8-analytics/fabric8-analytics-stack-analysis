import sys
import time
import os
from analytics_platform.kronos.src import config
import analytics_platform.kronos.pgm.src.pgm_constants as pgm_constants
from analytics_platform.kronos.pgm.src.pgm_pomegranate import PGMPomegranate
from util.analytics_platform_util import get_path_names
from util.data_store.s3_data_store import S3DataStore


def load_eco_to_kronos_dependency_dict(input_kronos_dependency_data_store, additional_path):
    eco_to_kronos_dependency_dict = dict()

    filenames = input_kronos_dependency_data_store.list_files(os.path.join(
                                                              additional_path,
                                                              pgm_constants.KD_OUTPUT_FOLDER))
    for filename in filenames:
        ecosystem = filename.split("/")[-1].split(".")[0].split("_")[-1]
        kronos_dependency_json = input_kronos_dependency_data_store.read_json_file(
            filename=filename)
        kronos_dependency_dict = dict(kronos_dependency_json)
        eco_to_kronos_dependency_dict[ecosystem] = kronos_dependency_dict

    return eco_to_kronos_dependency_dict


def load_eco_to_kronos_dependency_dict_s3(bucket_name, additional_path):
    input_data_store = S3DataStore(src_bucket_name=bucket_name,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict(
        input_kronos_dependency_data_store=input_data_store, additional_path=additional_path)

    return eco_to_kronos_dependency_dict


def load_user_eco_to_co_occerrence_matrix_dict(input_co_occurrence_data_store, additional_path):
    com_filenames = input_co_occurrence_data_store.list_files(os.path.join(
        additional_path, pgm_constants.COM_OUTPUT_FOLDER))

    temp_user_eco_to_co_occurrence_matrix_dict = dict()
    user_category_list = list()
    ecosystem_list = list()
    for com_filename in com_filenames:
        user_category = com_filename.split("/")[-2]
        if user_category not in user_category_list:
            user_category_list.append(user_category)
        ecosystem = com_filename.split("/")[-1].split(".")[0].split("_")[-1]
        if ecosystem not in ecosystem_list:
            ecosystem_list.append(ecosystem)
        co_occurrence_matrix = input_co_occurrence_data_store.read_json_file_into_pandas_df(
            com_filename)
        temp_user_eco_to_co_occurrence_matrix_dict[
            (user_category, ecosystem)] = co_occurrence_matrix

    user_eco_to_co_occurrence_matrix_dict = dict()

    for user_category in user_category_list:
        eco_to_co_occurrence_matrix_dict = dict()
        for ecosystem in ecosystem_list:
            eco_to_co_occurrence_matrix_dict[ecosystem] = \
                temp_user_eco_to_co_occurrence_matrix_dict[(user_category, ecosystem)]
        user_eco_to_co_occurrence_matrix_dict[user_category] = eco_to_co_occurrence_matrix_dict

    return user_eco_to_co_occurrence_matrix_dict


def train_and_save_kronos_list(input_kronos_dependency_data_store, input_co_occurrence_data_store,
                               output_data_store, additional_path):
    eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict(
        input_kronos_dependency_data_store=input_kronos_dependency_data_store,
        additional_path=additional_path)

    user_eco_to_cooccurrence_matrix_dict = load_user_eco_to_co_occerrence_matrix_dict(
        input_co_occurrence_data_store=input_co_occurrence_data_store,
        additional_path=additional_path)

    for user_category in user_eco_to_cooccurrence_matrix_dict.keys():
        eco_to_cooccurrence_matrix_dict = user_eco_to_cooccurrence_matrix_dict[user_category]
        for ecosystem in eco_to_cooccurrence_matrix_dict.keys():
            kronos_dependency_dict = eco_to_kronos_dependency_dict[ecosystem]
            cooccurrence_matrix_df = eco_to_cooccurrence_matrix_dict[ecosystem]
            kronos_model = PGMPomegranate.train(kronos_dependency_dict=kronos_dependency_dict,
                                                package_occurrence_df=cooccurrence_matrix_df)
            filename = os.path.join(pgm_constants.KRONOS_OUTPUT_FOLDER,str(user_category),
                                    "kronos_{}.json".format(str(ecosystem)))
            kronos_model.save(data_store=output_data_store,
                              filename=additional_path + filename)


def train_and_save_kronos_list_s3(training_data_url):

    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_kronos_dependency_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                     access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                     secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    input_cooccurrence_matrix_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                       access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                       secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    train_and_save_kronos_list(
        input_kronos_dependency_data_store=input_kronos_dependency_data_store,
        input_co_occurrence_data_store=input_cooccurrence_matrix_data_store,
        output_data_store=output_data_store, additional_path=additional_path)
