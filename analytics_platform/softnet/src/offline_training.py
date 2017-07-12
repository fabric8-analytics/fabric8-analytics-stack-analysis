from analytics_platform.softnet.src.cooccurrence_matrix_generator import CooccurrenceMatrixGenerator
from analytics_platform.softnet.src.kronos_dependency_generator import KronosDependencyGenerator
from util.data_store.s3_data_store import S3DataStore
from analytics_platform.softnet.src import config
from util.data_store.local_filesystem import LocalFileSystem
from analytics_platform.softnet.src.softnet_constants import *


def load_eco_to_kronos_dependency_dict(input_kronos_dependency_data_store):
    eco_to_kronos_dependency_dict = dict()
    kd_filenames = input_kronos_dependency_data_store.list_files(KD_OUTPUT_FOLDER)
    for kd_filename in kd_filenames:
        ecosystem = kd_filename.split("/")[-1].split(".")[0].split("_")[-1]
        kronos_dependency_obj = KronosDependencyGenerator.load(data_store=input_kronos_dependency_data_store,
                                                               filename=kd_filename)
        kronos_dependency_dict = kronos_dependency_obj.get_dictionary()
        eco_to_kronos_dependency_dict[ecosystem] = kronos_dependency_dict
    return eco_to_kronos_dependency_dict


def generate_and_save_kronos_dependency(input_gnosis_data_store, input_package_topic_data_store, output_data_store):
    gnosis_ref_arch_json = input_gnosis_data_store.read_json_file(filename=GNOSIS_RA_OUTPUT_PATH)
    gnosis_ref_arch_dict = dict(gnosis_ref_arch_json)
    package_topic_json = input_package_topic_data_store.read_json_file(GNOSIS_PTM_OUTPUT_PATH)
    package_topic_dict = dict(package_topic_json)

    eco_to_package_topic_dict = package_topic_dict[GNOSIS_PTM_PACKAGE_TOPIC_MAP]
    eco_to_topic_package_dict = package_topic_dict[GNOSIS_PTM_TOPIC_PACKAGE_MAP]

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
        filename = KD_OUTPUT_FOLDER + "/" + KD_BASE_FILENAME
        filename_formatted = filename.replace(".", "_" + ecosystem + ".")
        kronos_dependency_obj.save(data_store=output_data_store, filename=filename_formatted)


def generate_and_save_kronos_dependency_s3():
    input_gnosis_data_store = S3DataStore(src_bucket_name=config.AWS_SOFTNET_BUCKET,
                                          access_key=config.AWS_S3_ACCESS_KEY_ID,
                                          secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    input_package_topic_data_store = S3DataStore(src_bucket_name=config.AWS_GNOSIS_BUCKET,
                                                 access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                 secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=config.AWS_KRONOS_BUCKET,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    generate_and_save_kronos_dependency(input_gnosis_data_store=input_gnosis_data_store,
                                        input_package_topic_data_store=input_package_topic_data_store,
                                        output_data_store=output_data_store)


def generate_and_save_kronos_dependency_local():
    input_gnosis_data_store = LocalFileSystem("./analytics_platform/data/tusharma-softnet-data")
    input_package_topic_data_store = LocalFileSystem("./analytics_platform/data/tusharma-gnosis-data")
    output_data_store = LocalFileSystem("./analytics_platform/data/tusharma-kronos-data")
    generate_and_save_kronos_dependency(input_gnosis_data_store=input_gnosis_data_store,
                                        input_package_topic_data_store=input_package_topic_data_store,
                                        output_data_store=output_data_store)


def generate_and_save_cooccurrence_matrices(input_kronos_dependency_data_store, input_manifest_data_store,
                                            output_data_store):
    eco_to_kronos_dependency_dict = load_eco_to_kronos_dependency_dict(
        input_kronos_dependency_data_store=input_kronos_dependency_data_store)

    manifest_filenames = input_manifest_data_store.list_files(MANIFEST_FILEPATH)


    for manifest_filename in manifest_filenames:
        user_category = manifest_filename.split("/")[1]
        manifest_content_json_list = input_manifest_data_store.read_json_file(filename=manifest_filename)
        for manifest_content_json in manifest_content_json_list:
            manifest_content_dict = dict(manifest_content_json)
            ecosystem = manifest_content_dict[MANIFEST_ECOSYSTEM]
            kronos_dependency_dict = eco_to_kronos_dependency_dict[ecosystem]
            list_of_package_list = manifest_content_dict.get(MANIFEST_PACKAGE_LIST)
            cooccurrence_matrix_obj = CooccurrenceMatrixGenerator.generate_cooccurrence_matrix(
                kronos_dependency_dict=kronos_dependency_dict, list_of_package_list=list_of_package_list)
            output_filename = COM_OUTPUT_FOLDER + "/" + str(
                user_category) + "/" + "cooccurrence_matrix" + "_" + str(
                ecosystem) + ".json"
            cooccurrence_matrix_obj.save(data_store=output_data_store, filename=output_filename)



def generate_and_save_cooccurrence_matrices_s3():
    input_kronos_dependency_data_store = S3DataStore(src_bucket_name=config.AWS_KRONOS_BUCKET,
                                                     access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                     secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    input_manifest_data_store = S3DataStore(src_bucket_name=config.AWS_GNOSIS_BUCKET,
                                            access_key=config.AWS_S3_ACCESS_KEY_ID,
                                            secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    output_data_store = S3DataStore(src_bucket_name=config.AWS_KRONOS_BUCKET,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    generate_and_save_cooccurrence_matrices(input_kronos_dependency_data_store=input_kronos_dependency_data_store,
                                            input_manifest_data_store=input_manifest_data_store,
                                            output_data_store=output_data_store)


def generate_and_save_cooccurrence_matrices_local():
    input_kronos_dependency_data_store = LocalFileSystem("./analytics_platform/data/tusharma-kronos-data")

    input_manifest_data_store = LocalFileSystem("./analytics_platform/data/tusharma-gnosis-data")

    output_data_store = LocalFileSystem("./analytics_platform/data/tusharma-kronos-data")

    generate_and_save_cooccurrence_matrices(input_kronos_dependency_data_store=input_kronos_dependency_data_store,
                                            input_manifest_data_store=input_manifest_data_store,
                                            output_data_store=output_data_store)


if __name__ == '__main__':
    import time

    print(config.AWS_GNOSIS_BUCKET)

    t0 = time.time()

    print("kronos dependency started.")
    generate_and_save_kronos_dependency_s3()
    #generate_and_save_kronos_dependency_local()
    print("kronos dependency ended.")
    print("cooccurrence matrix generation started.")
    generate_and_save_cooccurrence_matrices_s3()
    #generate_and_save_cooccurrence_matrices_local()
    print("cooccurrence matrix generation ended.")

    print(time.time() - t0)
