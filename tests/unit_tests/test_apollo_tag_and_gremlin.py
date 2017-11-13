from analytics_platform.kronos.src import config
from util.data_store.local_filesystem import LocalFileSystem
from analytics_platform.kronos.apollo.src.apollo_tag_prune import TagListPruner
from analytics_platform.kronos.apollo.src.apollo_constants import APOLLO_TEMP_TEST_DATA
from analytics_platform.kronos.apollo.src.apollo_gremlin import GraphUpdater

from unittest import TestCase


class TestPruneAndUpdate(TestCase):

    # Test Class TagListPruner
    def test_generate_and_save_pruned_list_local(self):

        input_data_store = LocalFileSystem(
            "tests/data/data_apollo/")
        self.assertTrue(input_data_store is not None)

        output_data_store = LocalFileSystem(
            "tests/data/data_apollo/")
        self.assertTrue(output_data_store is not None)

        TagListPruner.get_prune_list(input_data_store,
                                     output_data_store,
                                     additional_path="",
                                     mode="test")

        saved_unknown_data_obj = LocalFileSystem(APOLLO_TEMP_TEST_DATA)
        self.assertTrue(saved_unknown_data_obj is not None)
        file_list = saved_unknown_data_obj.list_files()
        self.assertTrue(len(file_list) == 1)

        output_pruned_list_obj = TagListPruner.load(data_store=output_data_store,
                                                    filename="data_input_curated_package_topic/package_topic.json")
        self.assertTrue(output_pruned_list_obj is not None)
        output_result = output_pruned_list_obj.package_list
        self.assertTrue(output_result is not None)

        expected_pruned_list_obj = TagListPruner.load(data_store=output_data_store,
                                                      filename="data_input_curated_package_topic/package_topic.json")
        self.assertTrue(expected_pruned_list_obj is not None)
        expected_output_result = expected_pruned_list_obj.package_list
        self.assertTrue(expected_output_result is not None)

        for tag_generated, tag_expected in zip(output_result, expected_output_result):
            self.assertDictEqual(tag_generated, tag_expected)

# IMPORTANT: TestGraphUpdater needs to run after TestTagListPruner

    # Test class TestGraphUpdater(TestCase):
    def test_gremlin_updater_generate_payload(self):
        expected_pay_load = {'gremlin': "g.V().has('ecosystem', 'ruby').\
                                        has('name', within(str_packages)).\
                                        property('manual_tagging_required', true).\
                                        valueMap();",
                             'bindings': {
                                 'str_packages': ['service_identity']}}

        unknown_data_obj = LocalFileSystem(APOLLO_TEMP_TEST_DATA)
        self.assertTrue(unknown_data_obj is not None)
        file_list = unknown_data_obj.list_files()
        self.assertEqual(len(file_list), 1)
        file_name = file_list[0]
        self.assertEqual(file_name, "package_topic.json")

        data = unknown_data_obj.read_json_file(file_name)
        graph_obj = GraphUpdater(untagged_data=data)
        ecosystem = 'ruby'
        package_list = graph_obj.untagged_data[ecosystem]
        pay_load = graph_obj.generate_payload(ecosystem, package_list)
        self.assertDictEqual(pay_load, expected_pay_load)

        unknown_data_obj.remove_json_file(file_name)
        updated_file_list = unknown_data_obj.list_files()
        self.assertEqual(len(updated_file_list), 0)
