"""Constants for Manifest Formating."""

APOLLO_ECOSYSTEM = "ecosystem"
APOLLO_INPUT_PATH = "data_input_raw_manifest_file_list/"
APOLLO_PACKAGE_LIST = "package_list"
APOLLO_POMS = "all_poms_found"
APOLLO_DEP_LIST = "dependency_list"
APOLLO_WEIGHT_LIST = "github_weights"
APOLLO_EXTENDED_COUNT = "github_repeat_count"
APOLLO_STATS = "github_stats"

"""Constants for output Manifest file."""

MANIFEST_OUTPUT_FILEPATH = "data_input_manifest_file_list/"

"""Github weight values."""
GH_STAR = "stars"
GH_WATCH = "watching"
GH_FORK = "forks"
WT_STARS = 0.6
WT_FORKS = 0.3
WT_WATCHERS = 0.1
