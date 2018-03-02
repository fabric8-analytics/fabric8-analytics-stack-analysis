"""Functions used during offline training."""

from analytics_platform.kronos.apollo.src.apollo_tag_prune import TagListPruner
from analytics_platform.kronos.src import config
from .apollo_generate_frequency_dict import FrequencyDictGenerator
from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names


def train_and_save_pruned_tag_list_s3(training_data_url):
    """Return the clean package_topic present in the given s3 training URL.

    :param training_data_url: The Location where data is read from and written to.
    """
    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_package_topic_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                 access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                 secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    output_package_topic_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                                  access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                  secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    return TagListPruner.prune_tag_list(input_package_topic_data_store,
                                        output_package_topic_data_store,
                                        additional_path)


def generate_and_save_package_frequency_dict_s3(training_data_url):
    """Generate the frequency dictionary and store it into AWS S3."""
    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    frequency_dict_generator = FrequencyDictGenerator.create_frequency_generator(
        input_data_store=input_data_store,
        additional_path=additional_path)
    frequency_dict_generator.generate_and_save_frequency_dict(
        output_data_store=output_data_store,
        additional_path=additional_path)
