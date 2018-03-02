"""Generate the test, save it, and then call all relevant checkers."""

# NOTE: Currenlty works only with S3DataStore
import os

from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names

from evaluation_platform.uranus.src.generate_test_data import TestData
from evaluation_platform.uranus.src.alternate_testing import AlternateAccuracy
from evaluation_platform.uranus.src.companion_outlier_testing import CompanionOutlierAccuracy
from analytics_platform.kronos.src.config import (
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY)
from evaluation_platform.uranus.src.uranus_constants import (
    URANUS_EVALUATION_RESULT_PATH)


def generate_evaluate_test_s3(training_url, result_id):
    """Generate the test, save it, and then call all relevant checkers."""
    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_url)
    input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                   access_key=AWS_S3_ACCESS_KEY_ID,
                                   secret_key=AWS_S3_SECRET_ACCESS_KEY)
    output_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                    access_key=AWS_S3_ACCESS_KEY_ID,
                                    secret_key=AWS_S3_SECRET_ACCESS_KEY)
    generate_test(input_data_store, output_data_store, additional_path)
    test_kronos(training_url, result_id,
                input_data_store, output_data_store, additional_path)


def generate_test(input_data_store, output_data_store, additional_path):
    """Generate test from given input data store."""
    td = TestData()
    td.generate_attributes(input_data_store, additional_path)
    td.save_attributes(output_data_store, additional_path)


def test_kronos(training_url,
                result_id,
                input_data_store,
                output_data_store,
                additional_path):
    """Call the Alternate, Companion and Outlier Accuracy checker.

    :param training_url: Location where test data is loaded from.

    :return testing_result_dict: Accuracy/Evaluation metrices.
    """
    testing_result_dict = {"input_url": training_url}
    testing_result_dict["evaluation_id"] = result_id
    alt_acc_obj = AlternateAccuracy()
    alt_acc_obj.load_attributes(input_data_store, additional_path)
    testing_result_dict["Alternate"] = alt_acc_obj.alternate_precision()
    co_acc_obj = CompanionOutlierAccuracy()
    co_acc_obj.load_attributes(input_data_store, additional_path)
    testing_result_dict[
        "Number of Input Manifests"] = co_acc_obj.search_set_length
    result = co_acc_obj.companion_outlier_precision()
    testing_result_dict["Companion"] = result[0]
    testing_result_dict["Outlier"] = result[1]
    result_filename = os.path.join(
        additional_path,
        URANUS_EVALUATION_RESULT_PATH,
        result_id + ".json")
    output_data_store.write_json_file(result_filename, testing_result_dict)
