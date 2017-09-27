from analytics_platform.kronos.src import config
from analytics_platform.kronos.apollo.src.apollo_constants import *
from util.data_store.local_filesystem import LocalFileSystem
from analytics_platform.kronos.apollo.src.apollo_manifest_format import ManifestFormatter
from unittest import TestCase


class TestApolloManifestFormatter(TestCase):
    """Gnosis Formatter formats the raw manifest to match the Gnosis input format."""

    def __init__(self, *args, **kwargs):
        super(TestApolloManifestFormatter, self).__init__(*args, **kwargs)
        self.input_folder_name = "tests/data/data_apollo/input_raw_manifest"
        self.output_folder_name = "tests/data/data_apollo/output_clean_manifest"
        self.input_ecosystem = "maven"
        self.additional_path = ""

    def test_get_manifest(self):
        input_data_store = LocalFileSystem(self.input_folder_name)
        self.assertTrue(input_data_store is not None)
        output_data_store = LocalFileSystem(self.output_folder_name)
        self.assertTrue(output_data_store is not None)
        ManifestFormatter.get_clean_manifest(
            input_data_store, output_data_store, self.additional_path)

        output_file_path = MANIFEST_OUTPUT_FILEPATH + "1/clean_raw_manifest.json"
        expected_output_file_path = MANIFEST_OUTPUT_FILEPATH + \
            "1/expected_clean_raw_manifest.json"
        output_json = output_data_store.read_json_file(output_file_path)
        self.assertTrue(output_json is not None)
        expected_json = output_data_store.read_json_file(
            expected_output_file_path)
        self.assertTrue(expected_json is not None)

        self.assertDictEqual(output_json[0], expected_json[0])

    def test_github_stats(self):
        github_stats = {
            "stars": "1",
            "watching": "0",
            "forks": "3"
        }
        popularity_count = ManifestFormatter.get_popularity_count(github_stats)
        expected_popularity_count = int(round(int(github_stats[
            'stars']) * 0.6 + int(github_stats['forks']) * 0.3 + int(github_stats['watching']) * 0.1))
        self.assertEqual(popularity_count, expected_popularity_count)

    def test_get_aggregate_each_manifest_json(self):
        manifest_content_json = {
            "ecosystem": "maven",
            "package_list": [{
                "repo_name": "R1",
                "github_stats": {
                    "stars": "2500",
                    "watching": "3000",
                    "forks": "5000"
                },
                "all_poms_found": [{
                    "path_to_pom": "path to 1st pom found in the repo",
                    "dependency_list": ["1", "2", "3"]
                },
                    {
                    "path_to_pom": "path to the 2nd pom found in the repo",
                    "dependency_list": ["4", "5"]
                }
                ]
            }]}

        result_manifest_values = ManifestFormatter.get_aggregate_each_manifest_json(
            manifest_content_json)
        self.assertEqual(result_manifest_values["ecosystem"], "maven")
        self.assertEqual(result_manifest_values["github_repeat_count"], 6600)
        self.assertEqual(len(result_manifest_values["github_weights"]), 2)
        self.assertEqual(len(result_manifest_values["package_list"]), 2)
        self.assertEqual(result_manifest_values["github_weights"], [3300, 3300])
        self.assertEqual(result_manifest_values["package_list"], [
                         ["1", "2", "3"], ["4", "5"]])

    def test_output_file_name(self):
        output_data_store = LocalFileSystem(self.output_folder_name)
        self.assertTrue(output_data_store is not None)
        output_file_path = MANIFEST_OUTPUT_FILEPATH + "1/clean_raw_manifest.json"
        output_json = output_data_store.read_json_file(output_file_path)
        self.assertTrue(output_json is not None)

        obj_mf = ManifestFormatter(output_json)
        new_output_file_name = obj_mf.get_output_filename(
            output_file_path, self.additional_path)
        expected_output_file_name = MANIFEST_OUTPUT_FILEPATH + \
            "1/clean_clean_raw_manifest.json"
        self.assertEqual(new_output_file_name, expected_output_file_name)
