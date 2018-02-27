"""Kronos Dependency Generator."""

import analytics_platform.kronos.softnet.src.softnet_constants as softnet_constants
import util.softnet_util as softnet_utils
from util.data_store.local_filesystem import LocalFileSystem
import daiquiri
import logging

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


class KronosDependencyGenerator(object):
    """Kronos Dependency Generator.

    This class is responsible for generating the unweighted Kronos Dependency Graph.
    """

    def __init__(self, dictionary):
        """Instantiate Kronos Dependency Generator."""
        self._dictionary = dictionary

    @classmethod
    def generate_kronos_dependency(cls, gnosis_ref_arch_dict, package_to_topic_dict,
                                   topic_to_package_dict):
        """Generate a soft net.

        Soft net is a component class topic model created from the data
        available in the given data stores.

        :param data_store: Gnosis graph store.
        :param pkg_topic_store: Package Topic graph store.

        :return: Object of class KronosDependencyGenerator.
        """
        _logger.info("Started kronos dependency graph generation")
        package_list = list(package_to_topic_dict.keys())
        component_class_list = gnosis_ref_arch_dict.get(
            softnet_constants.GNOSIS_RA_COMPONENT_CLASS_LIST)

        component_class_to_package_edge_list, component_class_to_package_dict = \
            cls._generate_component_class_to_package_edge_list_and_dict(
                package_list, component_class_list, package_to_topic_dict)

        gnosis_ref_arch_intent_list = gnosis_ref_arch_dict.get(
            softnet_constants.GNOSIS_RA_INTENT_LIST)
        kronos_intent_list = gnosis_ref_arch_intent_list + component_class_list
        kronos_node_list = package_list + kronos_intent_list

        gnosis_ref_arch_edge_list = gnosis_ref_arch_dict.get(
            softnet_constants.GNOSIS_RA_EDGE_LIST)

        kronos_dependency_edge_list = gnosis_ref_arch_edge_list + \
            component_class_to_package_edge_list

        parent_tuple_list = softnet_utils.generate_parent_tuple_list(
                                                             kronos_node_list,
                                                             kronos_dependency_edge_list)
        parent_tuple_list_string = LocalFileSystem.convert_list_of_tuples_to_string(
            parent_tuple_list)
        similar_package_dict = cls._generate_similar_package_dict(package_to_topic_dict,
                                                                  topic_to_package_dict)

        kronos_dependency_dict = dict()
        kronos_dependency_dict[softnet_constants.KD_PACKAGE_LIST] = package_list
        kronos_dependency_dict[softnet_constants.KD_INTENT_LIST] = kronos_intent_list
        kronos_dependency_dict[
            softnet_constants.KD_INTENT_DEPENDENCY_MAP] = gnosis_ref_arch_dict.get(
            softnet_constants.GNOSIS_RA_DICT)
        kronos_dependency_dict[
            softnet_constants.KD_COMPONENT_DEPENDENCY_MAP] = component_class_to_package_dict
        kronos_dependency_dict[
            softnet_constants.KD_PARENT_TUPLE_LIST] = parent_tuple_list_string
        kronos_dependency_dict[softnet_constants.KD_EDGE_LIST] = kronos_dependency_edge_list
        kronos_dependency_dict[
            softnet_constants.KD_SIMILAR_PACKAGE_MAP] = similar_package_dict
        kronos_dependency_dict[
            softnet_constants.KD_PACKAGE_TO_TOPIC_MAP] = package_to_topic_dict
        _logger.info("Ended Kronos dependency graph generation")
        return KronosDependencyGenerator(kronos_dependency_dict)

    def save(self, data_store, filename):
        """Save the Soft Net: Component Class Topic Model.

        :param data_store: Data store to keep the model.
        :param file_name: Name of the file that will contain model.
        """
        kronos_dependency_dict = self._dictionary
        data_store.write_json_file(
            filename=filename, contents=kronos_dependency_dict)

    @classmethod
    def load(cls, data_store, filename):
        """Load the Soft Net: Component Class Topic Model.

        :param data_store: Data store to keep the model.
        :param file_name: Name of the file that contains model.
        """
        kronos_dependency_dict = data_store.read_json_file(filename)
        return KronosDependencyGenerator(kronos_dependency_dict)

    @classmethod
    def _generate_component_class_to_package_edge_list_and_dict(cls, package_list,
                                                                component_class_list,
                                                                package_topic_map,):
        _logger.info("Generating component class to package edge list")
        component_class_to_package_edge_list = list()
        component_class_to_package_dict = dict()
        for package in package_list:
            package_component_class_list = package_topic_map.get(package)
            for component_class in package_component_class_list:
                component_class_to_package_edge_list.append({
                    softnet_constants.EDGE_DICT_FROM: component_class,
                    softnet_constants.EDGE_DICT_TO: package
                })
                component_class_to_package_dict[component_class] = \
                    component_class_to_package_dict.get(component_class, []) + [package]
        for component_class in component_class_list:
            if component_class not in component_class_to_package_dict:
                component_class_to_package_dict[component_class] = []
        _logger.info("Done generating component class to package edge list")
        return component_class_to_package_edge_list, component_class_to_package_dict

    def get_dictionary(self):
        """Return the dictionary."""
        return self._dictionary

    @classmethod
    def _generate_similar_package_dict(cls, package_to_topic_dict, topic_to_package_dict):
        _logger.info("Started generating similar package dict")
        similar_package_dict = dict()
        for package in package_to_topic_dict:
            topics = package_to_topic_dict[package]
            list_of_package_list = list()
            for topic in topics:
                topic_package_list = topic_to_package_dict[topic]
                list_of_package_list.append(topic_package_list)
            distinct_package_list = list(set(sum(list_of_package_list, [])))
            distinct_package_list.remove(package)
            similar_package_dict_list = softnet_utils.get_similar_package_dict_list(
                package=package,
                package_list=distinct_package_list,
                package_to_topic_dict=package_to_topic_dict)
            similar_package_dict[package] = similar_package_dict_list
        _logger.info("Done generating similar package dict")
        return similar_package_dict
