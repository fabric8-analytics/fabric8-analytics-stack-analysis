"""Functions to perform offline training for Kronos softnet."""

import os
from analytics_platform.kronos.softnet.src.cooccurrence_matrix_generator import \
    CooccurrenceMatrixGenerator
from analytics_platform.kronos.softnet.src.kronos_dependency_generator import \
    KronosDependencyGenerator
import analytics_platform.kronos.softnet.src.softnet_constants as softnet_constants
from analytics_platform.kronos.src import config
from util.data_store.s3_data_store import S3DataStore
from util.analytics_platform_util import get_path_names


def load_eco_to_kronos_dependency_dict(input_kronos_dependency_data_store, additional_path):
    """Load the Kronos dependency dictionary from the selected storage."""
    eco_to_kronos_dependency_dict = dict()
    kd_filenames = input_kronos_dependency_data_store.list_files(
        os.path.join(additional_path, softnet_constants.KD_OUTPUT_FOLDER))
    for kd_filename in kd_filenames:
        ecosystem = kd_filename.split("/")[-1].split(".")[0].split("_")[-1]
        kronos_dependency_obj = KronosDependencyGenerator.load(
            data_store=input_kronos_dependency_data_store,
            filename=kd_filename)
        kronos_dependency_dict = kronos_dependency_obj.get_dictionary()
        eco_to_kronos_dependency_dict[ecosystem] = kronos_dependency_dict
    return eco_to_kronos_dependency_dict


def generate_and_save_kronos_dependency(input_gnosis_data_store, input_package_topic_data_store,
                                        output_data_store, additional_path):
    """Generate Kronos dependency and save it to the selected storage."""
    gnosis_ref_arch_json = \
        input_gnosis_data_store.read_json_file(filename=os.path.join(
                                               additional_path,
                                               softnet_constants.GNOSIS_RA_OUTPUT_PATH))
    gnosis_ref_arch_dict = dict(gnosis_ref_arch_json)
    package_topic_json = \
        input_package_topic_data_store.read_json_file(os.path.join(
                                                      additional_path,
                                                      softnet_constants.GNOSIS_PTM_OUTPUT_PATH))
    package_topic_dict = dict(package_topic_json)

    eco_to_package_topic_dict = package_topic_dict[softnet_constants.GNOSIS_PTM_PACKAGE_TOPIC_MAP]
    eco_to_topic_package_dict = package_topic_dict[softnet_constants.GNOSIS_PTM_TOPIC_PACKAGE_MAP]

    eco_to_kronos_dependency_dict = dict()

    for ecosystem in eco_to_package_topic_dict.keys():
        package_to_topic_dict = eco_to_package_topic_dict.get(ecosystem)
        topic_to_package_dict = eco_to_topic_package_dict.get(ecosystem)

        kronos_dependency_obj = KronosDependencyGenerator.generate_kronos_dependency(
            gnosis_ref_arch_dict=gnosis_ref_arch_dict,
            package_to_topic_dict=package_to_topic_dict,
            topic_to_package_dict=topic_to_package_dict)

        eco_to_kronos_dependency_dict[ecosystem] = kronos_dependency_obj

    for ecosystem in eco_to_kronos_dependency_dict.keys():
        kronos_dependency_obj = eco_to_kronos_dependency_dict[ecosystem]
        filename = os.path.join(softnet_constants.KD_OUTPUT_FOLDER,
                                softnet_constants.KD_BASE_FILENAME)
        filename_formatted = os.path.join(additional_path,
                                          filename.replace(".", "_{}.".format(ecosystem)))
        kronos_dependency_obj.save(data_store=output_data_store, filename=filename_formatted)


def generate_and_save_kronos_dependency_s3(training_data_url):
    """Generate Kronos dependency and save it to the AWS S3 storage."""
    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_gnosis_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                          access_key=config.AWS_S3_ACCESS_KEY_ID,
                                          secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    input_package_topic_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                 access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                 secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    generate_and_save_kronos_dependency(
        input_gnosis_data_store=input_gnosis_data_store,
        input_package_topic_data_store=input_package_topic_data_store,
        output_data_store=output_data_store, additional_path=additional_path)


def generate_and_save_cooccurrence_matrices(input_kronos_dependency_data_store,
                                            input_manifest_data_store, output_data_store,
                                            additional_path):
    """Generate and save cooccurrence matrices into the selected storage."""
    eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict(
        input_kronos_dependency_data_store=input_kronos_dependency_data_store,
        additional_path=additional_path)

    package_topic_json = \
        input_kronos_dependency_data_store.read_json_file(os.path.join(
                                                      additional_path,
                                                      softnet_constants.GNOSIS_PTM_OUTPUT_PATH))
    package_topic_dict = dict(package_topic_json)
    eco_to_package_topic_dict = package_topic_dict[softnet_constants.GNOSIS_PTM_PACKAGE_TOPIC_MAP]

    manifest_filenames = input_manifest_data_store.list_files(os.path.join(
        additional_path, softnet_constants.MANIFEST_FILEPATH))

    for manifest_filename in manifest_filenames:
        user_category = manifest_filename.split("/")[-2]
        manifest_content_json_list = input_manifest_data_store.read_json_file(
            filename=manifest_filename)
        for manifest_content_json in manifest_content_json_list:
            manifest_content_dict = dict(manifest_content_json)
            ecosystem = manifest_content_dict[softnet_constants.MANIFEST_ECOSYSTEM]
            kronos_dependency_dict = eco_to_kronos_dependency_dict[ecosystem]
            list_of_package_list = manifest_content_dict.get(
                softnet_constants.MANIFEST_PACKAGE_LIST)
            cooccurrence_matrix_obj = CooccurrenceMatrixGenerator.generate_cooccurrence_matrix(
                kronos_dependency_dict=kronos_dependency_dict,
                list_of_package_list=list_of_package_list,
                package_topic_map=eco_to_package_topic_dict[ecosystem])
            output_filename = os.path.join(softnet_constants.COM_OUTPUT_FOLDER,
                                           str(user_category),
                                           "cooccurrence_matrix_{}.json".format(str(ecosystem)))
            cooccurrence_matrix_obj.save(data_store=output_data_store,
                                         filename=os.path.join(additional_path, output_filename))


def generate_and_save_cooccurrence_matrices_s3(training_data_url):
    """Generate and save cooccurrence matrices into the AWS S3 storage."""
    input_bucket_name, output_bucket_name, additional_path = get_path_names(
        training_data_url)
    input_kronos_dependency_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                     access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                     secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    input_manifest_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                            access_key=config.AWS_S3_ACCESS_KEY_ID,
                                            secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    generate_and_save_cooccurrence_matrices(
        input_kronos_dependency_data_store=input_kronos_dependency_data_store,
        input_manifest_data_store=input_manifest_data_store,
        output_data_store=output_data_store, additional_path=additional_path)
