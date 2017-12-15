from analytics_platform.kronos.src.config import GREMLIN_REST_URL
from util.data_store.local_filesystem import LocalFileSystem
from util.request_util import get_session_retry
import json
import daiquiri
import logging

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


class GraphUpdater(object):
    """Update the Crowsouring graph with packages that need tags."""

    def __init__(self, untagged_data={}):
        """Instantiate Graph Updater."""
        self.untagged_data = untagged_data

    def generate_and_update_packages(self, apollo_temp_path):
        """For each package list, execute the gremlin queries if tagging needed."""
        local_data_obj = LocalFileSystem(apollo_temp_path)
        file_list = local_data_obj.list_files()
        for filename in file_list:
            data = local_data_obj.read_json_file(filename)
            self.untagged_data.clear()
            self.untagged_data = data
            self.update_graph()
            local_data_obj.remove_json_file(filename)

    def update_graph(self, chunk_size=100):
        """For each ecosystem, update the graph with packages that need tags."""
        for each_ecosystem in self.untagged_data:
            package_list = self.untagged_data[each_ecosystem]
            pck_len = len(package_list)
            for index in range(0, pck_len, chunk_size):
                sub_package_list = package_list[index:index + chunk_size]
                payload = self.generate_payload(
                    each_ecosystem, sub_package_list)
                self.execute_gremlin_dsl(payload)

    @staticmethod
    def generate_payload(ecosystem, package_list):
        """Generate the payload that is sent to graph.

        :param ecosystem: The ecosystem whose packages are to be updated.
        :param package_list: All the packages ina givne ecosystem that require tags.

        :return payload: The formated gremlin query.
        """
        # NOTE: This query assumes that the packages in the package_list are already
        # present in the graph.
        str_gremlin_query = "g.V().has('ecosystem', '{}')".format(ecosystem)
        str_gremlin_query += ".has('name', within(str_packages))."
        str_gremlin_query += "property('manual_tagging_required', true).valueMap();"
        payload = {
            'gremlin': str_gremlin_query,
            'bindings': {'str_packages': package_list}
        }
        return payload

    @staticmethod
    def execute_gremlin_dsl(payload):
        """Execute the gremlin query submitted in the pay load."""
        response = get_session_retry().post(
            GREMLIN_REST_URL, data=json.dumps(payload))
        if response.status_code != 200:
            _logger.error(
                "The rest failed with code {} for payload::\n {}".format(
                    response.status_Code,
                    payload))
