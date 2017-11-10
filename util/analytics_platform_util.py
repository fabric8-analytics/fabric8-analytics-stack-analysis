from nltk.tokenize import wordpunct_tokenize
from collections import Counter
import string

from util_constants import MAX_TAG_COUNT


def trunc_string_at(s, d, n1, n2):
    """Returns s truncated at the n'th occurrence of the delimiter, d"""
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
<<<<<<< 8bb3b4a7211c4c6e2d79d79e422b967066152047

    tags = set([tag.lower() for tag in wordpunct_tokenize(package_name) if
                tag not in string.punctuation and tag not in stop_words
                ])

    return list(tags)[:MAX_TAG_COUNT]
=======
    name_parts = package_name.split(":")[:2]
    if len(name_parts) > 1:
        # ecosystem is maven, at least based on naming scheme
        tags_artifact = [tag for tag in wordpunct_tokenize(name_parts[1]) if
                         tag not in string.punctuation]
        tags_group = [tag for tag in wordpunct_tokenize(name_parts[0]) if
                      tag not in string.punctuation and tag not in
                      ['org', 'com', 'io', 'ch', 'cn']]
        tags = set(tags_artifact + tags_group)
    else:
        # return the tokenized package name
        tags = set([tag for tag in wordpunct_tokenize(package_name)
                    if tag not in string.punctuation])
    tags = [tag.lower()
            for tag in tags][:MAX_TAG_COUNT]
    # Make sure there are no duplicates
    return list(set(tags))


def get_path_names(training_data_url):
    """Return the bucket name and additiona path.
        :param training_data)url: The location where data is read from and written to."""

    input_bucket_name = trunc_string_at(training_data_url, "/", 2, 3)
    output_bucket_name = trunc_string_at(training_data_url, "/", 2, 3)
    additional_path = trunc_string_at(training_data_url, "/", 3, -1)
    return input_bucket_name, output_bucket_name, additional_path
>>>>>>> Add code for pruning taglist in apollo module.
