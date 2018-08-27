"""Script to start offline evaluation."""

# NOTE: Currently works only with S3DataStore
from uuid import uuid1
import sys
import time

from evaluation_platform.uranus.src.evaluate_data import generate_evaluate_test_s3
import daiquiri
import logging

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        training_data_url = "s3://dev-stack-analysis-clean-data/maven/github/"
        result_id = str(uuid1())
        _logger.info("No env provided, using default")
        _logger.info("Evalutaion result id = {}".format(result_id))
    else:
        training_data_url = sys.argv[1]
        result_id = sys.argv[2]
        _logger.info("Env Provided")

    _logger.info("S3 URL : {}".format(training_data_url))
    t0 = time.time()
    _logger.info("Kronos Evaluation started")
    generate_evaluate_test_s3(training_url=training_data_url,
                              result_id=result_id)
    _logger.info(
        "Kronos Evaluation Ended in {} seconds".format(time.time() - t0))
