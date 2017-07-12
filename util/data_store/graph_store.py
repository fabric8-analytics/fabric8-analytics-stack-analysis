from abstract_data_store import AbstractDataStore
import json


class GraphStore(AbstractDataStore):

    def __init__(self, src_graph_store_url):
        self.url = src_graph_store_url

    def get_name(self):
        return "Graph:" + self.url

    def read_json_file(self, filename):
        """TBD"""
        return None

    def list_files(self, prefix=None):
        """TBD"""
        return None

    def write_json_file(self, filename, contents):
        """Write JSON file into data source"""
