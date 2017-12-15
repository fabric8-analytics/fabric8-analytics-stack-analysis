from analytics_platform.kronos.src import config
from util.data_store.local_filesystem import LocalFileSystem
from analytics_platform.kronos.apollo.src.apollo_tag_prune import TagListPruner
from analytics_platform.kronos.apollo.src.apollo_constants import APOLLO_TEMP_TEST_DATA
from analytics_platform.kronos.apollo.src.apollo_gremlin import GraphUpdater

from unittest import TestCase


class TestPruneAndUpdate(TestCase):

    def test_generate_and_save_pruned_list_local(self):

        input_data_store = LocalFileSystem(
            "tests/data/data_apollo/")
        assert input_data_store

        output_data_store = LocalFileSystem(
            "tests/data/data_apollo/")
        assert output_data_store

        TagListPruner.prune_tag_list(input_data_store,
                                     output_data_store,
                                     additional_path="",
                                     apollo_temp_path=APOLLO_TEMP_TEST_DATA)

        saved_unknown_data_obj = LocalFileSystem(APOLLO_TEMP_TEST_DATA)
        assert saved_unknown_data_obj
        file_list = saved_unknown_data_obj.list_files()
        self.assertEqual(len(file_list), 1)

        output_pruned_list_obj = TagListPruner.load(
            data_store=output_data_store,
            filename="data_input_curated_package_topic/package_topic.json")
        assert output_pruned_list_obj
        output_result = output_pruned_list_obj.package_list
        assert output_result

        expected_pruned_list_obj = TagListPruner.load(
            data_store=output_data_store,
            filename="data_input_curated_package_topic/expected_output.json")
        assert expected_pruned_list_obj
        expected_output_result = expected_pruned_list_obj.package_list
        assert expected_output_result

        for tag_generated, tag_expected in zip(output_result, expected_output_result):
            self.assertDictEqual(tag_generated, tag_expected)

        output_data_store.remove_json_file(
            filename="data_input_curated_package_topic/package_topic.json")

        unknown_data_obj = LocalFileSystem(APOLLO_TEMP_TEST_DATA)
        assert unknown_data_obj
        file_list = unknown_data_obj.list_files()
        self.assertEqual(len(file_list), 1)
        filename = file_list[0]
        self.assertEqual(filename, "package_topic.json")

        test_data = unknown_data_obj.read_json_file(filename)
        self.assertEqual(set(test_data['maven']), set())
        self.assertEqual(set(test_data['npm']), set())
        self.assertEqual(set(test_data['pypi']), set())
        self.assertEqual(set(test_data['ruby']), set(['service_identity']))

        unknown_data_obj.remove_json_file(filename)
        updated_file_list = unknown_data_obj.list_files()
        self.assertEqual(len(updated_file_list), 0)

    def test_gremlin_updater_generate_payload(self):

        gremlin_query = "g.V().has('ecosystem', 'ruby')."
        gremlin_query += "has('name', within(str_packages))."
        gremlin_query += "property('manual_tagging_required', true).valueMap();"
        expected_pay_load = {
            'gremlin': gremlin_query,

            'bindings': {
                'str_packages': ['service_identity']}}

        ecosystem = 'ruby'
        package_list = ['service_identity']
        pay_load = GraphUpdater.generate_payload(ecosystem, package_list)
        self.assertDictEqual(pay_load, expected_pay_load)
