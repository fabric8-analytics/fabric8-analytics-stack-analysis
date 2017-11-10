from analytics_platform.kronos.src import config
from analytics_platform.kronos.apollo.src.apollo_constants import (
    APOLLO_ECOSYSTEM,
    APOLLO_INPUT_PATH,
    APOLLO_PACKAGE_LIST,
    PACKAGE_LIST_OUTPUT_FILEPATH,
    MAX_TAG_COUNT)

from util.analytics_platform_util import create_tags_for_package
from collections import Counter


class TagListPruner(object):
    """Formats the raw package_list to prune it for Gnosis consumption."""

    def __init__(self, pruned_package_list):
        """Instantiate Tag List Pruner."""

        self.package_list = pruned_package_list

    @staticmethod
    def get_prune_list(input_package_topic_data_store,
                       output_package_topic_data_store,
                       additional_path):
        """Generate the clean aggregated package_topic list as required by Gnosis.

        :param input_package_topic_data_store: The Data store to pick the package_topic files from.
        :param output_package_topic_data_store: The Data store to save the clean package_topic to.
        :param additional_path: The directory to pick the package_topic files from."""

        package_list_files = input_package_topic_data_store.list_files(
            additional_path + APOLLO_INPUT_PATH)
        for package_file in package_list_files:
            content_json_list = input_package_topic_data_store.read_json_file(
                filename=package_file)
            TagListPruner.clean_each_file(package_file,
                                          content_json_list,
                                          output_package_topic_data_store, additional_path)

        return None

    def save(self, data_store, filename):
        """Saves the package_topic object in json format.

        :param data_store: Data store to save package_topic in.
        :param filename: the file into which the package_topic is to be saved."""

        data_store.write_json_file(
            filename=filename, contents=self.package_list)

    @classmethod
    def load(cls, data_store, filename):
        """Load the Package Topic Model.

        :param data_store: Data store to keep the model.
        :param file_name: Name of the file that contains model."""

        dictionary = data_store.read_json_file(filename)
        return TagListPruner(pruned_package_list=dictionary)

    @classmethod
    def generate_save_obj(cls, result_package_topic_json,
                          package_file,
                          output_package_topic_data_store,
                          additional_path):
        """Create and save the object of TagListPruner class.

           :param result_package_topic_json: The clean package_topic json to be saved.
           :param package_file: The output filename for clean package_topic.
           :param output_data_store: The output data store where clean package_topics are saved."""

        package_topic_formatter_obj = TagListPruner(result_package_topic_json)
        output_filename = additional_path + \
            PACKAGE_LIST_OUTPUT_FILEPATH + package_file.split("/")[-1]
        package_topic_formatter_obj.save(
            output_package_topic_data_store, output_filename)

    @classmethod
    def prune_tag_list_max_count(cls, package_list):
        """Prune the  package list based on maximum count.

           :param package_list: The complete package_list.

           :return pruned_package_list: The prune and clean package_list."""

        pruned_package_list = {}
        stop_word = set(['vertx', 'spring', 'java', 'apache',
                         'vert.x', 'io', 'com', 'commons', 'algorithms', 'language'])
        word_count = Counter()
        for package_name, tag_list in package_list.items():
            temp_list = set()
            for tag in tag_list:
                tag = tag.lower()
                if tag not in stop_word:
                    word_count[tag] += 1
                temp_list.add(tag)
            package_list[package_name] = list(temp_list)

        for package_name, tag_list in package_list.items():
            if len(tag_list) == 0:
                tag_list = create_tags_for_package(
                    package_name)
                # TODO: Update Crowsourcing DB with package_name for tagging.
            if len(tag_list) > MAX_TAG_COUNT:
                package_tag_count = Counter()
                for tag in tag_list:
                    package_tag_count[tag] = word_count[tag]
                tag_list = [package_tag[0].lower()
                            for package_tag in package_tag_count.most_common(4)]
            pruned_package_list[package_name] = tag_list
        return pruned_package_list

    @classmethod
    def clean_each_file(cls, package_file,
                        content_json_list,
                        output_package_topic_data_store,
                        additional_path):
        """Prepare the clean package_topic data and save it.

           :param package_file: The raw package_topic file name.
           :param content_json_list: The raw package_topic json content.
           :param output_package_topic_data_store: Save clean package_topic json here."""

        result_package_topic_json = []
        for package_topic_content_json in content_json_list:
            ecosystem = package_topic_content_json.get(APOLLO_ECOSYSTEM)
            package_tag_list = package_topic_content_json.get(
                APOLLO_PACKAGE_LIST)
            pruned_package__topic_list = TagListPruner.prune_tag_list_max_count(
                package_tag_list)
            temp_eco_values = {APOLLO_ECOSYSTEM: ecosystem}
            temp_eco_values[APOLLO_PACKAGE_LIST] = pruned_package__topic_list
            result_package_topic_json.append(temp_eco_values)
        # TODO: use singleton object, with updated package_topic_list
        TagListPruner.generate_save_obj(result_package_topic_json,
                                        package_file,
                                        output_package_topic_data_store,
                                        additional_path)
