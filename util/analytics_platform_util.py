from nltk.tokenize import wordpunct_tokenize
import string
import os

from analytics_platform.kronos.gnosis.src.gnosis_constants import (
    MANIFEST_FILEPATH, MANIFEST_PACKAGE_LIST)
from util.util_constants import MAX_TAG_COUNT


def trunc_string_at(s, d, n1, n2):
    """Return s truncated at the n'th occurrence of the delimiter, d."""
    if n2 > 0:
        result = d.join(s.split(d, n2)[n1:n2])
    else:
        result = d.join(s.split(d, n2)[n1:])
        if not result.endswith("/"):
            result += "/"
    return result


def create_tags_for_package(package_name):
    """Create tags for a package based on its name."""
    stop_words = set(['org', 'com', 'io', 'ch', 'cn'])
    tags = []
    tags = set([tag.lower() for tag in wordpunct_tokenize(package_name) if
                tag not in string.punctuation and tag not in stop_words
                ])

    return list(tags)[:MAX_TAG_COUNT]


def get_path_names(training_data_url):
    """Return the bucket name and additiona path.

    :param training_data)url: The location where data is read from and written to.
    """
    input_bucket_name = trunc_string_at(training_data_url, "/", 2, 3)
    output_bucket_name = trunc_string_at(training_data_url, "/", 2, 3)
    additional_path = trunc_string_at(training_data_url, "/", 3, -1)
    return input_bucket_name, output_bucket_name, additional_path


def load_package_list(input_data_store, additional_path):
    """Load the manifest files and returns list of set of packages.

    Load the manifest files from the input_data_store and
    returns an aggregated list of set of packages.

    :param input_data_store: Data store to read the manifest files from.
    :param additional_path: Indicates path inside the data store for the manifests.
    :return: list of package set.
    """
    all_list_of_package_set = list()
    manifest_filenames = input_data_store.list_files(
        os.path.join(additional_path, MANIFEST_FILEPATH))
    for manifest_filename in manifest_filenames:
        manifest_content_json_list = input_data_store.read_json_file(
            filename=manifest_filename)
        for manifest_content_json in manifest_content_json_list:
            manifest_content_dict = dict(manifest_content_json)
            list_of_package_list = manifest_content_dict.get(MANIFEST_PACKAGE_LIST, [])
            for package_list in list_of_package_list:
                all_list_of_package_set.append(set(package_list))
    return all_list_of_package_set
