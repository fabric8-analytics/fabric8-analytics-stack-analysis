"""Class that represents local filesystem-bases data storage."""

import fnmatch
import os
import pickle
import pandas as pd
from pomegranate import BayesianNetwork
import json
import ast
import numpy as np

from util.data_store.abstract_data_store import AbstractDataStore


class LocalFileSystem(AbstractDataStore):
    """Class that represents local filesystem-bases data storage."""

    def __init__(self, src_dir):
        """Set the directory used as a data storage."""
        self.src_dir = src_dir

    def get_name(self):
        """Get the name that identifies the storage."""
        return "Local filesytem dir: " + self.src_dir

    def list_files(self, prefix=None):
        """List all the files in the source directory."""
        list_filenames = []
        for root, dirs, files in os.walk(self.src_dir):
            for basename in files:
                if fnmatch.fnmatch(basename, "*.json"):
                    filename = os.path.join(root, basename)
                    if prefix is None:
                        filename = filename[len(self.src_dir):]
                        list_filenames.append(filename)
                    elif filename.startswith(os.path.join(self.src_dir, prefix)):
                        filename = filename[len(self.src_dir):]
                        list_filenames.append(filename)
        list_filenames.sort()
        return list_filenames

    def remove_json_file(self, filename):
        """Remove JSON file from the data_input source file path."""
        return os.remove(os.path.join(self.src_dir, filename))

    def read_json_file(self, filename):
        """Read JSON file from the data_input source."""
        return LocalFileSystem.byteify(json.load(open(os.path.join(self.src_dir, filename))))

    def read_all_json_files(self):
        """Read all the files from the data_input source."""
        list_filenames = self.list_files(prefix=None)
        list_contents = []
        for file_name in list_filenames:
            contents = self.read_json_file(filename=file_name)
            list_contents.append((file_name, contents))
        return list_contents

    def write_json_file(self, filename, contents):
        """Write JSON file into data_input source."""
        with open(os.path.join(self.src_dir, filename), 'w') as outfile:
            json.dump(contents, outfile)
        return None

    def upload_file(self, _src, _target):
        """Upload file into data store."""
        # self.bucket.upload_file(src, target)
        return None

    def download_file(self, _src, _target):
        """Download file from data store."""
        # self.bucket.download_file(src, target)
        return None

    def read_json_file_into_pandas_df(self, filename):
        """Read and parse JSON file."""
        return pd.read_json(os.path.join(self.src_dir, filename), dtype=np.int8)

    def write_pandas_df_into_json_file(self, data, filename):
        """Write the structured data into the JSON file."""
        data.to_json(os.path.join(self.src_dir, filename))

    def write_pomegranate_model(self, model, filename):
        """Serialize the model into file."""
        with open(os.path.join(self.src_dir, filename), 'wb') as f:
            # IMPORTANT: Set pickle.HIGHEST_PROTOCOL only  after complete porting to
            # Python3
            pickle.dump(model.to_json(), f, protocol=2)

    def read_pomegranate_model(self, filename):
        """Deserialize the model from the file."""
        with open(os.path.join(self.src_dir, filename), 'rb') as ik:
            model = BayesianNetwork.from_json(pickle.load(ik))
        return model

    @classmethod
    def byteify(cls, input):
        """Convert any decoded JSON object from Unicode strings to UTF-8-encoded byte strings."""
        # see the discussion and the original function here:
        # https://stackoverflow.com/questions/956867/how-to-get-string-objects-instead-of-unicode-from-json#16373377
        if isinstance(input, dict):
            return {LocalFileSystem.byteify(key): LocalFileSystem.byteify(value)
                    for key, value in input.items()}
        elif isinstance(input, list):
            return [LocalFileSystem.byteify(element) for element in input]
        else:
            return input

    @classmethod
    def convert_list_of_tuples_to_string(cls, tuple_list):
        """Convert list of tuples into string."""
        assert cls is not None  # just make checkers happy
        string_value = str(tuple_list)
        return string_value

    @classmethod
    def convert_string_to_list_of_tuples(cls, tuple_list_string):
        """Convert string into list of tuples (if possible)."""
        # TODO: this function is duplicatd in the pgm_util.py, refactoring needed here
        assert cls is not None  # just make checkers happy
        return list(ast.literal_eval(tuple_list_string))
