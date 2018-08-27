"""Class that represents Graph DB data storage."""

# TODO: the implementation is not complete, so this class is not usable ATM

from abstract_data_store import AbstractDataStore


class GraphStore(AbstractDataStore):
    """Class that represents Graph DB data storage."""

    def __init__(self, src_graph_store_url):
        """Initialize the session to the Graph database."""
        # TODO: not implemented
        self.url = src_graph_store_url

    def get_name(self):
        """Get the name that identifies the storage."""
        return "Graph:" + self.url

    def read_json_file(self, filename):
        """Read JSON file from the Graph database."""
        # TODO: not implemented
        return None

    def list_files(self, prefix=None):
        """List all the files in the Graph database."""
        # TODO: not implemented
        return None

    def write_json_file(self, filename, contents):
        """Write JSON file into data source."""
        # TODO: not implemented
        pass
