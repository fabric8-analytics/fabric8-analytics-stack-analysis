"""Utility functions for handling package lists, compute similarity score etc."""

import numpy as np
import pandas as pd

import analytics_platform.kronos.softnet.src.softnet_constants as softnet_constants


def generate_parent_tuple_list(node_list, edge_dict_list):
    """Generate parent tuple list."""
    child_to_parent_dict = dict()
    for edge_dict in edge_dict_list:
        child = edge_dict[softnet_constants.EDGE_DICT_TO]
        parent = edge_dict[softnet_constants.EDGE_DICT_FROM]
        if child in child_to_parent_dict:
            temp_parent_list = child_to_parent_dict[child]
            temp_parent_list += [node_list.index(parent)]
            child_to_parent_dict[child] = temp_parent_list
        else:
            child_to_parent_dict[child] = [node_list.index(parent)]
    parent_tuple_list = list()
    for node in node_list:
        if node in child_to_parent_dict:
            parent_tuple_list.append(tuple(child_to_parent_dict[node]))
        else:
            parent_tuple_list.append(())
    return tuple(parent_tuple_list)


def get_similar_package_dict_list(package, package_list, package_to_topic_dict):
    """Compute the dictionary with similar packages sorted by similarity score."""
    topic_list_1 = package_to_topic_dict[package]
    package_score_dict_list = list()
    for package_2 in package_list:
        topic_list_2 = package_to_topic_dict[package_2]
        actual_topic_list = [
            x[len(softnet_constants.GNOSIS_PTM_TOPIC_PREFIX):] for x in topic_list_2]
        similarity_score = calculate_similarity_score(topic_list_1, topic_list_2)
        package_score_dict_list.append(
            {softnet_constants.KD_PACKAGE_NAME: package_2,
             softnet_constants.KD_SIMILARITY_SCORE: similarity_score,
             softnet_constants.KD_TOPIC_LIST: actual_topic_list})
    sorted_package_score_dict_list = sorted(
        package_score_dict_list,
        key=lambda x: x[softnet_constants.KD_SIMILARITY_SCORE],
        reverse=True)
    return sorted_package_score_dict_list


def calculate_similarity_score(topic_list_1, topic_list_2):
    """Calculate the similarity score between two topic lists."""
    average_length = float(len(topic_list_1) + len(topic_list_2)) / 2
    intersection_set = set.intersection(set(topic_list_1), set(topic_list_2))
    similarity_score = float(len(intersection_set)) / average_length
    return similarity_score


def create_empty_pandas_df(rowsize, columns_list):
    """Create empty Panda data frame (filled by zeros)."""
    zero_data = np.zeros(shape=(rowsize, len(columns_list)), dtype=np.int8)
    df = pd.DataFrame(zero_data, columns=columns_list)
    return df
