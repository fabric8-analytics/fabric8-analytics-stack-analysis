import sys
import time

from analytics_platform.kronos.src import config
from analytics_platform.kronos.gnosis.src.gnosis_package_topic_model import GnosisPackageTopicModel
from analytics_platform.kronos.gnosis.src.gnosis_ref_arch import GnosisReferenceArchitecture
from gnosis_constants import *
from util.data_store.s3_data_store import S3DataStore


def trunc_string_at(s, d, n1, n2):
    """Returns s truncated at the n'th occurrence of the delimiter, d"""
    if n2 > 0:
        result = d.join(s.split(d, n2)[n1:n2])
    else:
        result = d.join(s.split(d, n2)[n1:])
        if not result.endswith("/"):
            result += "/"
    return result


def train_and_save_gnosis_ref_arch(input_data_store, output_data_store, additional_path, fp_min_support_count,
                                   fp_intent_topic_count_threshold,
                                   fp_num_partition):
    gnosis_ref_arch_obj = GnosisReferenceArchitecture.train(data_store=input_data_store,
                                                            additional_path=additional_path,
                                                            min_support_count=fp_min_support_count,
                                                            min_intent_topic_count=fp_intent_topic_count_threshold,
                                                            fp_num_partition=fp_num_partition)
    gnosis_ref_arch_obj.save(output_data_store, additional_path + GNOSIS_RA_OUTPUT_PATH)
    return None


def train_and_save_gnosis_ref_arch_s3(training_data_url, fp_min_support_count,
                                      fp_intent_topic_count_threshold,
                                      fp_num_partition):
    """
    Trains the Ref Arch Gnosis and saves the Gnosis model in S3
    :return: None
    """
    input_bucket_name = trunc_string_at(training_data_url, "/", 2, 3)
    output_bucket_name = trunc_string_at(training_data_url, "/", 2, 3)
    additional_path = trunc_string_at(training_data_url, "/", 3, -1)

    input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    train_and_save_gnosis_ref_arch(input_data_store=input_data_store, output_data_store=output_data_store,
                                   additional_path=additional_path, fp_min_support_count=fp_min_support_count,
                                   fp_intent_topic_count_threshold=fp_intent_topic_count_threshold,
                                   fp_num_partition=fp_num_partition)
    return None


def generate_and_save_gnosis_package_topic_model(input_data_store, output_data_store, additional_path):
    """
    Trains the package to topic map as well as topic to package map.
    :param input_data_store: source data store
    :param output_data_store: destination data store
    :param type: "curate" or "train"
    :return: None
    """

    gnosis_package_topic_model_obj = GnosisPackageTopicModel.curate(data_store=input_data_store,
                                                                    filename=additional_path + GNOSIS_PTM_INPUT_PATH)
    gnosis_package_topic_model_obj.save(data_store=output_data_store, filename=additional_path + GNOSIS_PTM_OUTPUT_PATH)
    return None


def generate_and_save_gnosis_package_topic_model_s3(training_data_url):
    input_bucket_name = trunc_string_at(training_data_url, "/", 2, 3)
    output_bucket_name = trunc_string_at(training_data_url, "/", 2, 3)
    additional_path = trunc_string_at(training_data_url, "/", 3, -1)

    input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    generate_and_save_gnosis_package_topic_model(input_data_store=input_data_store, output_data_store=output_data_store,
                                                 additional_path=additional_path
                                                 )


if __name__ == '__main__':
    if len(sys.argv) < 2:
        training_data_url = "s3://perf-gsk-data/python/machine-learning/"
        fp_min_support_count = 8
        fp_intent_topic_count_threshold = 3
        fp_num_partition = 12
        print("no env")
    else:
        training_data_url = sys.argv[1]
        fp_min_support_count = int(sys.argv[2])
        fp_intent_topic_count_threshold = int(sys.argv[3])
        fp_num_partition = int(sys.argv[4])
        print("env")

    print(training_data_url)
    print(fp_min_support_count)
    print(fp_intent_topic_count_threshold)
    print(fp_num_partition)

    t0 = time.time()
    generate_and_save_gnosis_package_topic_model_s3(training_data_url=training_data_url)
    train_and_save_gnosis_ref_arch_s3(training_data_url=training_data_url, fp_min_support_count=fp_min_support_count,
                                      fp_intent_topic_count_threshold=fp_intent_topic_count_threshold,
                                      fp_num_partition=fp_num_partition)
    print(time.time() - t0)
