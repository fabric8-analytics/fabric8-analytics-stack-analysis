from analytics_platform.kronos.src import config
from util.data_store.local_filesystem import LocalFileSystem
from analytics_platform.kronos.apollo.src.apollo_tag_prune import TagListPruner
from analytics_platform.kronos.apollo.src.apollo_constants import (
        PACKAGE_LIST_INPUT_CURATED_FILEPATH)
from unittest import TestCase


class TestTagListPruner(TestCase):

    def test_generate_and_save_pruned_list_local(self):
        input_data_store = LocalFileSystem(
            "tests/data/data_apollo/")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem(
            "tests/data/data_apollo/")
        self.assertTrue(output_data_store is not None)

        TagListPruner.prune_tag_list(input_data_store,
                                     output_data_store,
                                     additional_path="")

        output_pruned_list_obj = TagListPruner.load(
            data_store=output_data_store,
            filename=PACKAGE_LIST_INPUT_CURATED_FILEPATH + "package_topic.json")
        self.assertTrue(output_pruned_list_obj is not None)
        output_result = output_pruned_list_obj.package_list
        self.assertTrue(output_result is not None)
        expected_output_json = TagListPruner.load(
            data_store=output_data_store,
            filename=PACKAGE_LIST_INPUT_CURATED_FILEPATH + "expected_output.json")
        self.assertTrue(expected_output_json is not None)
        expected_output_result = expected_output_json.package_list
        self.assertTrue(expected_output_result is not None)

        for tag_generated, tag_expected in zip(output_result, expected_output_result):
            self.assertDictEqual(tag_generated, tag_expected)
