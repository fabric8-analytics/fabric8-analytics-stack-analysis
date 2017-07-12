from analytics_platform.gnosis.src import config
from analytics_platform.gnosis.src.gnosis_ref_arch import GnosisReferenceArchitecture
from analytics_platform.gnosis.src.gnosis_package_topic_model import GnosisPackageTopicModel
from gnosis_constants import *
from util.data_store.local_filesystem import LocalFileSystem
from util.data_store.s3_data_store import S3DataStore
import time


def train_and_save_gnosis_ref_arch(input_data_store, output_data_store):
    gnosis_ref_arch_obj = GnosisReferenceArchitecture.train(data_store=input_data_store)

    gnosis_ref_arch_obj.save(output_data_store, GNOSIS_RA_OUTPUT_PATH)
    return None


def train_and_save_gnosis_ref_arch_s3():
    """
    Trains the Ref Arch Gnosis and saves the Gnosis model in S3
    :return: None
    """
    input_data_store = S3DataStore(src_bucket_name=config.AWS_GNOSIS_BUCKET,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=config.AWS_SOFTNET_BUCKET,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    train_and_save_gnosis_ref_arch(input_data_store=input_data_store, output_data_store=output_data_store)
    return None


def train_and_save_gnosis_ref_arch_local():
    """
    Trains the Ref Arch Gnosis and saves the Gnosis model in local filesystem

    :param type: "curate" or "train"
    :return: None
    """
    input_data_store = LocalFileSystem("analytics_platform/data/tusharma-gnosis-data")
    output_data_store = LocalFileSystem("analytics_platform/data/tusharma-softnet-data")

    train_and_save_gnosis_ref_arch(input_data_store=input_data_store, output_data_store=output_data_store)
    return None


def generate_and_save_gnosis_package_topic_model(input_data_store, output_data_store):
    """
    Trains the package to topic map as well as topic to package map.
    :param input_data_store: source data store
    :param output_data_store: destination data store
    :param type: "curate" or "train"
    :return: None
    """

    gnosis_package_topic_model_obj = GnosisPackageTopicModel.curate(data_store=input_data_store,
                                                                    filename=GNOSIS_PTM_INPUT_PATH)
    gnosis_package_topic_model_obj.save(data_store=output_data_store, filename=GNOSIS_PTM_OUTPUT_PATH)
    return None


def generate_and_save_gnosis_package_topic_model_s3():
    input_data_store = S3DataStore(src_bucket_name=config.AWS_GNOSIS_BUCKET,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=config.AWS_GNOSIS_BUCKET,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    generate_and_save_gnosis_package_topic_model(input_data_store=input_data_store, output_data_store=output_data_store,
                                                 )


def generate_and_save_gnosis_package_topic_model_local():
    input_data_store = LocalFileSystem("analytics_platform/data/tusharma-gnosis-data")
    output_data_store = LocalFileSystem("analytics_platform/data/tusharma-gnosis-data")

    generate_and_save_gnosis_package_topic_model(input_data_store=input_data_store, output_data_store=output_data_store)


if __name__ == '__main__':
    t0 = time.time()
    generate_and_save_gnosis_package_topic_model_s3()
    #generate_and_save_gnosis_package_topic_model_local()
    train_and_save_gnosis_ref_arch_s3()
    #train_and_save_gnosis_ref_arch_local()
    print(time.time() - t0)
