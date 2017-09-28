from analytics_platform.kronos.gnosis.src.offline_training import (
    generate_and_save_gnosis_package_topic_model_s3, train_and_save_gnosis_ref_arch_s3)
from analytics_platform.kronos.softnet.src.offline_training import (
    generate_and_save_kronos_dependency_s3, generate_and_save_cooccurrence_matrices_s3)
from analytics_platform.kronos.pgm.src.offline_training import train_and_save_kronos_list_s3
from analytics_platform.kronos.apollo.src.apollo_manifest_format import ManifestFormatter
import sys
import time

if __name__ == '__main__':
    if len(sys.argv) < 2:
        training_data_url = "s3://dev-stack-analysis-clean-data/python/github/"
        fp_min_support_count = 45
        fp_intent_topic_count_threshold = 3
        fp_num_partition = 12
        print("no env")
    else:
        training_data_url = sys.argv[1]
        fp_min_support_count = int(sys.argv[2])
        fp_intent_topic_count_threshold = int(sys.argv[3])
        fp_num_partition = int(sys.argv[4])
        print("env")

    print("S3 URL : ", training_data_url)
    print()

    t0 = time.time()
    print("Manifest Preprocessing started")
    ManifestFormatter.get_data_s3_store(training_data_url=training_data_url)
    print("Manifest Preprocessing Ended in ", time.time() - t0, " seconds")

    t0 = time.time()
    print("Gnosis Training Started")
    generate_and_save_gnosis_package_topic_model_s3(training_data_url=training_data_url)
    train_and_save_gnosis_ref_arch_s3(
        training_data_url=training_data_url,
        fp_min_support_count=fp_min_support_count,
        fp_intent_topic_count_threshold=fp_intent_topic_count_threshold,
        fp_num_partition=fp_num_partition)
    print("Gnosis Training Ended in ", time.time() - t0, " seconds")

    t0 = time.time()
    print("Softnet Training Started")
    generate_and_save_kronos_dependency_s3(training_data_url=training_data_url)
    generate_and_save_cooccurrence_matrices_s3(training_data_url=training_data_url)
    print("Softnet Training Ended in ", time.time() - t0, " seconds")

    t0 = time.time()
    print("Kronos Training Started")
    train_and_save_kronos_list_s3(training_data_url=training_data_url)
    print("Kronos Training Ended in ", time.time() - t0, " seconds")
