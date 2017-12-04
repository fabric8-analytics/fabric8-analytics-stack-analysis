import sys
import time
import os
from analytics_platform.kronos.gnosis.src.gnosis_package_topic_model import GnosisPackageTopicModel
from analytics_platform.kronos.gnosis.src.gnosis_ref_arch import GnosisReferenceArchitecture
from analytics_platform.kronos.src import config
import analytics_platform.kronos.gnosis.src.gnosis_constants as gnosis_constants
from util.analytics_platform_util import get_path_names
from util.data_store.s3_data_store import S3DataStore


def train_and_save_gnosis_ref_arch(input_data_store, output_data_store, additional_path,
                                   fp_min_support_count,
                                   fp_intent_topic_count_threshold,
                                   fp_num_partition):
    gnosis_ref_arch_obj = GnosisReferenceArchitecture.train(
        data_store=input_data_store,
        additional_path=additional_path,
        min_support_count=fp_min_support_count,
        min_intent_topic_count=fp_intent_topic_count_threshold,
        fp_num_partition=fp_num_partition)
    gnosis_ref_arch_obj.save(
        output_data_store, os.path.join(additional_path, gnosis_constants.GNOSIS_RA_OUTPUT_PATH))
    return None


def train_and_save_gnosis_ref_arch_s3(training_data_url, fp_min_support_count,
                                      fp_intent_topic_count_threshold,
                                      fp_num_partition):
    """
    Trains the Ref Arch Gnosis and saves the Gnosis model in S3
    :return: None
    """

    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    train_and_save_gnosis_ref_arch(input_data_store=input_data_store,
                                   output_data_store=output_data_store,
                                   additional_path=additional_path,
                                   fp_min_support_count=fp_min_support_count,
                                   fp_intent_topic_count_threshold=fp_intent_topic_count_threshold,
                                   fp_num_partition=fp_num_partition)
    return None


def generate_and_save_gnosis_package_topic_model(input_data_store, output_data_store,
                                                 additional_path):
    """Trains the package to topic map as well as topic to package map.

    :param input_data_store: source data store.
    :param output_data_store: destination data store.
    :param type: "curate" or "train". """

    gnosis_package_topic_model_obj = GnosisPackageTopicModel.curate(
        data_store=input_data_store,
        filename=os.path.join(additional_path, gnosis_constants.GNOSIS_PTM_INPUT_PATH),
        additional_path=additional_path)
    gnosis_package_topic_model_obj.save(
        data_store=output_data_store,
        filename=os.path.join(additional_path, gnosis_constants.GNOSIS_PTM_OUTPUT_PATH))

    return None


def generate_and_save_gnosis_package_topic_model_s3(training_data_url):
    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    generate_and_save_gnosis_package_topic_model(input_data_store=input_data_store,
                                                 output_data_store=output_data_store,
                                                 additional_path=additional_path)
