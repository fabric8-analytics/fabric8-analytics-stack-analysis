"""Constants for Kronos Dependency i.e. KD."""

KD_PACKAGE_LIST = "package_list"
KD_INTENT_LIST = "intent_list"
KD_INTENT_DEPENDENCY_MAP = "intent_dependency_map"
KD_COMPONENT_DEPENDENCY_MAP = "component_dependency_map"
KD_PARENT_TUPLE_LIST = "parent_tuple_list"
KD_EDGE_LIST = "kronos_edge_list"
KD_SIMILAR_PACKAGE_MAP = "similar_package_dict"
KD_PACKAGE_NAME = "package_name"
KD_SIMILARITY_SCORE = "similarity_score"
KD_PACKAGE_TO_TOPIC_MAP = "package_to_topic_dict"
KD_TOPIC_LIST = "topic_list"
KD_PACKAGE_FREQUENCY = "data_outlier_manifest_file/element_frequency.json"

KD_OUTPUT_FOLDER = "data_kronos_dependency"
COM_OUTPUT_FOLDER = "data_co_occurrence_matrix"

"""Constants for Kronos Model."""

COMPONENT_RESULT_DICT = "package_result_dict"
INTENT_RESULT_DICT = "intent_result_dict"
KRONOS_OUTLIER_PROBABILITY_THRESHOLD_VALUE = 0.9
KRONOS_UNKNOWN_PACKAGE_RATIO_THRESHOLD_VALUE = 0.2
KRONOS_COMPANION_PACKAGE_COUNT_THRESHOLD_VALUE = 4
KRONOS_ALTERNATE_PACKAGE_COUNT_THRESHOLD_VALUE = 2
KRONOS_OUTLIER_COUNT_THRESHOLD_VALUE = 2
KRONOS_OUTPUT_FOLDER = "data_kronos_user_eco"
KRONOS_OUTLIER_PACKAGE_NAME = "package_name"
KRONOS_FREQUENCY_COUNT = "frequency_count"
KRONOS_COMPANION_PACKAGE_NAME = "package_name"
KRONOS_COMPANION_PROBABILITY = "cooccurrence_probability"
KRONOS_COMPANION_INTENT_NAME = "intent_name"
KRONOS_ALTERNATE_PACKAGES = "alternate_packages"
KRONOS_COMPANION_PACKAGES = "companion_packages"
KRONOS_OUTLIER_PACKAGES = "outlier_package_list"
KRONOS_MISSING_PACKAGES = "missing_packages"
KRONOS_PACKAGE_TO_TOPIC_DICT = "package_to_topic_dict"

"""Constants for Input Request for scoring."""

KRONOS_SCORE_ECOSYSTEM = "ecosystem"
KRONOS_SCORE_USER_PERSONA = "user_persona"
KRONOS_SCORE_PACKAGE_LIST = "package_list"
KRONOS_COMPANION_PACKAGE_COUNT_THRESHOLD_NAME = "comp_package_count_threshold"
KRONOS_ALTERNATE_PACKAGE_COUNT_THRESHOLD_NAME = "alt_package_count_threshold"
KRONOS_OUTLIER_PROBABILITY_THRESHOLD_NAME = "outlier_probability_threshold"
KRONOS_UNKNOWN_PACKAGE_RATIO_THRESHOLD_NAME = "unknown_packages_ratio_threshold"
KRONOS_OUTLIER_COUNT_THRESHOLD_NAME = "outlier_package_count_threshold"

"""Constants for Gnosis PTM."""

GNOSIS_PTM_TOPIC_PREFIX = "c_"
