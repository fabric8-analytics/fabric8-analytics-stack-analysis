# NOTE: Currenlty works only with S3DataStore
import json
import time

from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names

from analytics_platform.kronos.src.config import (
    AWS_S3_ACCESS_KEY_ID,
    AWS_S3_SECRET_ACCESS_KEY)
from analytics_platform.kronos.uranus.src.alternate_testing import AlternateAccuracy
from analytics_platform.kronos.uranus.src.companion_outlier_testing import CompanionOutlierAccuracy

import daiquiri
import logging

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


def test_kronos_s3(training_url):
    """ Call the Alternate, Companion and Outlier Accuracy checker.

        :param training_url: Location where test data is loaded from.

        :return testing_result_dict: Accuracy/Evaluation metrices."""

    input_bucket_name, _, additional_path = get_path_names(
        training_url)
    input_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                   access_key=AWS_S3_ACCESS_KEY_ID,
                                   secret_key=AWS_S3_SECRET_ACCESS_KEY)
    alt_acc_obj = AlternateAccuracy()
    alt_acc_obj.load_attributes(input_data_store, additional_path)
    testing_result_dict = {"Alternate": alt_acc_obj.alternate_precision()}
    # TODO: Parallelise between calling alternate and companion testing.
    co_acc_obj = CompanionOutlierAccuracy()
    co_acc_obj.load_attributes(input_data_store, additional_path)
    result = co_acc_obj.companion_outlier_precision()
    testing_result_dict["Companion"] = result[0]
    testing_result_dict["Outlier"] = result[1]
    _logger.info(
        "Kronos Evalution completed for data at {}".format(training_url))
    return testing_result_dict


def main():
    """Keep running the script and call testing function once a request has arrived.
       Once the test results are available, update the request dictionary with output.
       And finally update the request queue."""

    while(True):
        # TODO: parallelise for multiple request.
        with open("/tmp/queue.json", "r") as f:
            queue = json.load(f)
        if len(queue) == 0:
            time.sleep(5)
            continue
        request_id = queue.pop(0)

        with open("/tmp/request_dict.json", "r") as f:
            request_dict = json.load(f)
        training_url = request_dict.get(request_id, {}).get("input")
        if training_url is not None:
            _logger.info("Kronos Evaluation started for id {}".format(
                request_id))
            request_dict[request_id][
                "output"] = test_kronos_s3(training_url)
        else:
            request_dict[request_id][
                "output"] = None
        with open("/tmp/request_dict.json", "w") as f:
            json.dump(request_dict, f)
        with open("/tmp/queue.json", "w") as f:
            json.dump(queue, f)
        time.sleep(5)


if __name__ == '__main__':
    main()
