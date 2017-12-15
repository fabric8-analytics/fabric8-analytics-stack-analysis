from analytics_platform.kronos.apollo.src.apollo_tag_prune import TagListPruner
from analytics_platform.kronos.apollo.src.apollo_gremlin import GraphUpdater
from analytics_platform.kronos.src.config import (
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY)
from analytics_platform.kronos.apollo.src.apollo_constants import APOLLO_TEMP_TEST_DATA
from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names


def train_and_save_pruned_tag_list_s3(
        training_data_url,
        apollo_temp_path=APOLLO_TEMP_TEST_DATA):
    """Return the clean package_topic present in the given s3 training URL.

    :param training_data_url: The Location where data is read from and written to.
    :param apollo_temp_path: The location where to be updated packages
    will be temporarily stored.
    """
    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_package_topic_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                 access_key=AWS_S3_ACCESS_KEY_ID,
                                                 secret_key=AWS_S3_SECRET_ACCESS_KEY)
    output_package_topic_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                                  access_key=AWS_S3_ACCESS_KEY_ID,
                                                  secret_key=AWS_S3_SECRET_ACCESS_KEY)
    return TagListPruner.prune_tag_list(input_package_topic_data_store,
                                        output_package_topic_data_store,
                                        additional_path,
                                        apollo_temp_path)


def generate_and_update_query(apollo_temp_path=APOLLO_TEMP_TEST_DATA):
    """Update the graph DB by inserting packages listed at the temp path.
    Once updated the package list files are cleared.

    :param apollo_temp_path: The location where to be updated packages will be temporarily stored.
    """
    graph_obj = GraphUpdater()
    graph_obj.generate_and_update_packages(apollo_temp_path)
