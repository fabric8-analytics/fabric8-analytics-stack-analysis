"""Tests for the class FrequencyDictGenerator."""

from unittest import TestCase

from analytics_platform.kronos.apollo.src.apollo_generate_frequency_dict import \
    FrequencyDictGenerator
from analytics_platform.kronos.pgm.src.pgm_constants import KD_PACKAGE_FREQUENCY
from util.data_store.local_filesystem import LocalFileSystem


class TestFrequencyDictGenerator(TestCase):
    """Tests for the class FrequencyDictGenerator."""

    def test_generate_and_save_package_frequency_dict_local(self):
        """Test the frequency dict generator and its serialization."""
        input_data_store = LocalFileSystem(
            src_dir="tests/data/data_gnosis/input-ra-data/"
        )
        self.assertIsNotNone(input_data_store)

        output_data_store = LocalFileSystem(
            src_dir="tests/data/data_apollo/"
        )

        self.assertIsNotNone(output_data_store)

        frequency_dict_generator = FrequencyDictGenerator.create_frequency_generator(
            input_data_store=input_data_store,
            additional_path="")

        self.assertIsNotNone(frequency_dict_generator)

        frequency_dict_generator.generate_and_save_frequency_dict(
            output_data_store=output_data_store,
            additional_path="")

        frequency_dict = output_data_store.read_json_file(filename=KD_PACKAGE_FREQUENCY)

        self.assertIsNotNone(frequency_dict)
