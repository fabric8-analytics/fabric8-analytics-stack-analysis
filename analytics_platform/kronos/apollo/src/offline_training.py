from analytics_platform.kronos.apollo.src.apollo_tag_prune import TagListPruner
from analytics_platform.kronos.src import config
from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names


def train_and_save_pruned_tag_list_s3(training_data_url):
    """Return the clean package_topic present in the given s3 training URL.
        :param training_data_url: The Location where data is read from and written to."""

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
