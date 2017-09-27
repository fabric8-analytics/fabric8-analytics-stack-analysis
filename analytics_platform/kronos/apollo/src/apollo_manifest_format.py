from analytics_platform.kronos.src import config
from analytics_platform.kronos.apollo.src.apollo_constants import *
from util.data_store.s3_data_store import S3DataStore
from util.data_store.local_filesystem import LocalFileSystem


class ManifestFormatter(object):
    """Gnosis Formatter formats the raw manifest to match the Gnosis input format."""

    def __init__(self, clean_manifest_json):
        """Instantiate Gnosis Formatter."""

        self.manifest_json = clean_manifest_json

    @staticmethod
    def get_clean_manifest(input_manifest_data_store, output_manifest_data_store, additional_path):
        """Generate the clean aggregated manifest list as required by Gnosis.

        :param input_manifest_data_store: The Data store to pick the manifest files from.
        :param output_manifest_data_store: The Data store to save the clean manifest files to.
        :param additional_path: The directory to pick the manifest files from.

        :return: None."""

        manifest_filenames = input_manifest_data_store.list_files(
            additional_path + APOLLO_INPUT_PATH)
        for manifest_filename in manifest_filenames:
            manifest_content_json_list = input_manifest_data_store.read_json_file(
                filename=manifest_filename)
            ManifestFormatter.clean_each_file(manifest_filename,
                                              manifest_content_json_list,
                                              output_manifest_data_store, additional_path)

        return None

    @classmethod
    def load_manifest_s3(cls, input_bucket_name, output_bucket_name, additional_path):
        """Generate the aggregated manifest list for a given ecosystem for S3 datasource.

        :param input_bucket_name: The bucket where the manifest files are stored.
        :param additional_path: The directory to pick the manifest files from.
        :param input_ecosystem: The ecosystem for which the aggregated manifest list will be saved.

        :return: RecommendationValidator object."""

        # Create a S3 object
        input_manifest_data_store = S3DataStore(src_bucket_name=input_bucket_name,
                                                access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
        output_manifest_data_store = S3DataStore(src_bucket_name=output_bucket_name,
                                                 access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                 secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
        return cls.get_clean_manifest(input_manifest_data_store, output_manifest_data_store, additional_path)

    @classmethod
    def load_manifest_local(cls, input_folder_name, output_folder_name, additional_path):
        """Generate the aggregated manifest list for a given ecosystem from
        LocalFileSystem datasource.

        :param input_folder: The main directory where the manifest files are stored.
        :param additional_path: The directory to pick the manifest files from.
        :param input_ecosystem: The ecosystem for which the aggregated manifest list will be saved.

        :return: RecommendationValidator object."""

        # Create a LocalFile object
        input_manifest_data_store = LocalFileSystem(src_dir=input_folder_name)
        output_manifest_data_store = LocalFileSystem(src_dir=output_folder_name)
        return cls.get_clean_manifest(input_manifest_data_store, output_manifest_data_store, additional_path)

    def save(self, data_store, filename):
        """Saves the Manifest object in json format.

        :param data_store: Data store to save manifest in.
        :param filename: the file into which the manifest is to be saved."""

        data_store.write_json_file(
            filename=filename, contents=self.get_dictionary())
        return None

    def get_dictionary(self):
        """Return the clean manifest_json."""

        return self.manifest_json

    def get_output_filename(self, manifest_filename, additional_path):
        """Generate output filename from given input filename.

           :param manifest_filename: The input file name.

           :return output_filename: The corresponding output filename."""

        user_category = manifest_filename.split("/")[-2]
        name = "clean_" + manifest_filename.split("/")[-1]
        output_filename = additional_path + \
            MANIFEST_OUTPUT_FILEPATH + user_category + "/" + name
        return output_filename

    @classmethod
    def get_popularity_count(cls, github_stats):
        """Return the popularity count as per the defined weights.

           :param github_stats: Github stats for a repo

           :return popularity_count: Weighted popularity of the the repo."""

        forks = int(github_stats.get(GH_FORK, 0))
        stars = int(github_stats.get(GH_STAR, 0))
        watchers = int(github_stats.get(GH_WATCH, 0))
        popularity_count = int(
            round(WT_STARS * stars + WT_FORKS * forks + WT_WATCHERS * watchers))
        return popularity_count

    @classmethod
    def generate_save_obj(cls, result_manifest_json, manifest_filename, output_manifest_data_store, additional_path):
        """Create and save the object of ManifestFormatter class.

           :param result_manifest_json: The clean manifets json to be saved.
           :param manifest_filename: The output filename for clean manifest.
           :param output_data_store: The output data store where clean manifests are saved."""

        manifest_formatter_obj = ManifestFormatter(result_manifest_json)
        output_filename = manifest_formatter_obj.get_output_filename(
            manifest_filename, additional_path)
        manifest_formatter_obj.save(
            output_manifest_data_store, output_filename)

    @classmethod
    def aggregate_each_pom(cls, package_list):
        """Aggregate package list and weights for each pom per repo.

           :param package_list: The complete package_list per manifest.

           :return aggregated_package_list: The list of all dependency list.
           :return aggregated_package_weight: The weights for each dependency list.
           :return aggregated_count: The sume of weights across the dependency list."""

        aggregated_package_list = []
        aggregated_package_weight = []
        aggregated_count = 0

        for each_repo in package_list:
            github_stats = each_repo.get(APOLLO_STATS, {})
            popularity_count = ManifestFormatter.get_popularity_count(
                github_stats)
            for each_pom in each_repo.get(APOLLO_POMS, []):
                dep_list = each_pom.get(APOLLO_DEP_LIST)
                if dep_list is None:
                    continue
                aggregated_package_list.append(dep_list)
                aggregated_package_weight.append(popularity_count)
                aggregated_count += popularity_count

        return aggregated_package_list, aggregated_package_weight, aggregated_count

    @classmethod
    def get_aggregate_each_manifest_json(cls, manifest_content_json):
        """Create a dict of clean values per manifest_json.

           :param manifest_content_json: The raw manifest content to be cleaned. 

           :return: Clean dict of values."""

        temp_eco_values = {}
        ecosystem = manifest_content_json.get(APOLLO_ECOSYSTEM)
        package_list = manifest_content_json.get(APOLLO_PACKAGE_LIST)
        aggregated_package_list, aggregated_package_weight, aggregated_count = ManifestFormatter.aggregate_each_pom(
            package_list)
        temp_eco_values[APOLLO_ECOSYSTEM] = ecosystem
        temp_eco_values[APOLLO_PACKAGE_LIST] = aggregated_package_list
        temp_eco_values[APOLLO_WEIGHT_LIST] = aggregated_package_weight
        temp_eco_values[APOLLO_EXTENDED_COUNT] = aggregated_count
        return temp_eco_values

    @classmethod
    def clean_each_file(cls, manifest_filename, manifest_content_json_list, output_manifest_data_store, additional_path):
        """Prepare the clean manifest data and save it.

           :param manifest_filename: The raw manifest file name.
           :param manifest_content_json_list: The raw manifest json content.
           :param output_manifest_data_store: The data source to save clean manifest json."""

        result_manifest_json = []
        for manifest_content_json in manifest_content_json_list:
            temp_eco_values = ManifestFormatter.get_aggregate_each_manifest_json(
                manifest_content_json)
            result_manifest_json.append(temp_eco_values)
        # TODO: use singleton object, with updated manifest_list
        ManifestFormatter.generate_save_obj(
            result_manifest_json, manifest_filename, output_manifest_data_store, additional_path)


if __name__ == '__main__':
    ManifestFormatter.load_manifest_local(input_folder_name="tests/data/data_apollo/input_raw_manifest",
                                          output_folder_name="tests/data/data_apollo/output_clean_manifest",
                                          additional_path="")
    # ManifestFormatter.load_manifest_s3(input_bucket_name=config.AWS_BUCKET_NAME,
    #                                    output_bucket_name=config.AWS_BUCKET_NAME,
    #                                    additional_path=config.KRONOS_MODEL_PATH)
