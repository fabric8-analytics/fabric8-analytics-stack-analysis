from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names
from analytics_platform.kronos.uranus.src.generate_test_data import TestData
from analytics_platform.kronos.src.config import (
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY)


def generate_and_save_test_data_s3(training_data_url):
    """Generate and save the test data files only for S3 datastore.

    :param training_data_url: The url where the data is read from and written into."""

    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                   access_key=AWS_S3_ACCESS_KEY_ID,
                                   secret_key=AWS_S3_SECRET_ACCESS_KEY)
    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=AWS_S3_ACCESS_KEY_ID,
                                    secret_key=AWS_S3_SECRET_ACCESS_KEY)
    td = TestData()
    td.generate_attributes(input_data_store, additional_path)
    td.save_attributes(output_data_store, additional_path)
