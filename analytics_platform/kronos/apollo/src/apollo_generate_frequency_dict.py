from analytics_platform.kronos.pgm.src.pgm_constants import KD_PACKAGE_FREQUENCY
import os
from util.analytics_platform_util import load_package_list
from collections import Counter


class FrequencyDictGenerator(object):
    """Generates package to frequency dictionary."""

    def __init__(self, manifest_file):
        """Initialize FrequencyDictGenerator with manifest_file of ecosystem.

        :param manifest_file: The manifest.json file for intended ecosystem.
        """
        self.manifest_file = manifest_file

    @classmethod
    def create_frequency_generator(cls, input_data_store, additional_path):
        """Read the manifest file from the data store and creates a FrequencyDictGenerator object.

        :param input_data_store: Data store to read the manifest file from.
        :param additional_path: Path to the manifest.json file inside the input_data_store.
        :return: FrequencyDictGenerator object
        """
        manifest_file = load_package_list(input_data_store=input_data_store,
                                          additional_path=additional_path)
        return cls(manifest_file=manifest_file)

    def generate_and_save_frequency_dict(self, output_data_store, additional_path):
        """Generate and save frequency dictionary from the manifest_file.

        :param output_data_store: Data store to write the frequency dict to.
        :param additional_path: Path to store the frequency dict inside the output_data_store.
        """
        package_to_frequency_counter = Counter(package for package_set in self.manifest_file
                                               for package in package_set)
        package_to_frequency_dict = dict(package_to_frequency_counter)

        output_data_store.write_json_file(
            filename=os.path.join(additional_path, KD_PACKAGE_FREQUENCY),
            contents=package_to_frequency_dict
        )
