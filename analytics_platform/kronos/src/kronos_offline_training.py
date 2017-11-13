from analytics_platform.kronos.gnosis.src.offline_training import (
    generate_and_save_gnosis_package_topic_model_s3, train_and_save_gnosis_ref_arch_s3)
from analytics_platform.kronos.softnet.src.offline_training import (
    generate_and_save_kronos_dependency_s3, generate_and_save_cooccurrence_matrices_s3)
from analytics_platform.kronos.pgm.src.offline_training import train_and_save_kronos_list_s3
from analytics_platform.kronos.apollo.src.offline_training import (
    train_and_save_pruned_tag_list_s3,
    generate_and_update_query)

import sys
import time
import daiquiri
import logging

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        training_data_url = "s3://dev-stack-analysis-clean-data/maven/github/"
        fp_min_support_count = 45
        fp_intent_topic_count_threshold = 3
        fp_num_partition = 12
        _logger.info("No env provided, using default")
    else:
        training_data_url = sys.argv[1]
        fp_min_support_count = int(sys.argv[2])
        fp_intent_topic_count_threshold = int(sys.argv[3])
        fp_num_partition = int(sys.argv[4])
        _logger.info("Env Provided")

    _logger.info("S3 URL : {}".format(training_data_url))

    t0 = time.time()
    _logger.info("Tag List Preprocess started")
    train_and_save_pruned_tag_list_s3(
        training_data_url=training_data_url, mode="code")
    generate_and_update_query(mode="code")
    _logger.info(
        "tag List Preprocessing Ended in {} seconds".format(time.time() - t0))

    t0 = time.time()
    _logger.info("Gnosis Training Started")
    generate_and_save_gnosis_package_topic_model_s3(
        training_data_url=training_data_url)
    train_and_save_gnosis_ref_arch_s3(
        training_data_url=training_data_url,
        fp_min_support_count=fp_min_support_count,
        fp_intent_topic_count_threshold=fp_intent_topic_count_threshold,
        fp_num_partition=fp_num_partition)
    _logger.info("Gnosis Training Ended in {} seconds".format(time.time() - t0))

    t0 = time.time()
    _logger.info("Softnet Training Started")
    generate_and_save_kronos_dependency_s3(training_data_url=training_data_url)
    _logger.info(
        "Dependency graph Training Ended in {} seconds".format(time.time() - t0))

    t0 = time.time()
    generate_and_save_cooccurrence_matrices_s3(
        training_data_url=training_data_url)
    _logger.info(
        "Co-occurence matrix Training Ended in {} seconds".format(time.time() - t0))

    t0 = time.time()
    _logger.info("Kronos Training Started")
    train_and_save_kronos_list_s3(training_data_url=training_data_url)
    _logger.info("Kronos Training Ended in {} seconds".format(time.time() - t0))
