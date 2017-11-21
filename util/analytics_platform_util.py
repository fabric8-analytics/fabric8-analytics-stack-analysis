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

    tags = set([tag.lower() for tag in wordpunct_tokenize(package_name) if
                tag not in string.punctuation and tag not in stop_words
                ])

    return list(tags)[:MAX_TAG_COUNT]
