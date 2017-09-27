from analytics_platform.kronos.src import config
from analytics_platform.kronos.apollo.src.apollo_constants import *

from unittest import TestCase


class TestApolloConstants(TestCase):

    def test_apollo_manifest_constants(self):
        self.assertTrue(isinstance(APOLLO_ECOSYSTEM, str))
        self.assertEqual(APOLLO_ECOSYSTEM, "ecosystem")

        self.assertTrue(isinstance(APOLLO_INPUT_PATH, str))
        self.assertEqual(APOLLO_INPUT_PATH,
                         "data_input_raw_manifest_file_list/")

        self.assertTrue(isinstance(APOLLO_PACKAGE_LIST, str))
        self.assertEqual(APOLLO_PACKAGE_LIST, "package_list")

        self.assertTrue(isinstance(APOLLO_POMS, str))
        self.assertEqual(APOLLO_POMS, "all_poms_found")

        self.assertTrue(isinstance(APOLLO_DEP_LIST, str))
        self.assertEqual(APOLLO_DEP_LIST, "dependency_list")

        self.assertTrue(isinstance(APOLLO_WEIGHT_LIST, str))
        self.assertEqual(APOLLO_WEIGHT_LIST, "github_weights")

        self.assertTrue(isinstance(APOLLO_EXTENDED_COUNT, str))
        self.assertEqual(APOLLO_EXTENDED_COUNT, "github_repeat_count")

        self.assertTrue(isinstance(APOLLO_STATS, str))
        self.assertEqual(APOLLO_STATS, "github_stats")

    def test_manifest_output_path(self):
        self.assertTrue(isinstance(MANIFEST_OUTPUT_FILEPATH, str))
        self.assertEqual(MANIFEST_OUTPUT_FILEPATH,
                         "data_input_manifest_file_list/")

    def test_github_weight_values(self):
        self.assertTrue(isinstance(GH_STAR, str))
        self.assertEqual(GH_STAR, "stars")

        self.assertTrue(isinstance(GH_FORK, str))
        self.assertEqual(GH_FORK, "forks")

        self.assertTrue(isinstance(GH_WATCH, str))
        self.assertEqual(GH_WATCH, "watching")

        self.assertTrue(0 <= WT_STARS <= 1)
        self.assertEqual(WT_STARS, 0.6)
        self.assertTrue(0 <= WT_FORKS <= 1)
        self.assertEqual(WT_FORKS, 0.3)
        self.assertTrue(0 <= WT_WATCHERS <= 1)
        self.assertEqual(WT_WATCHERS, 0.1)
