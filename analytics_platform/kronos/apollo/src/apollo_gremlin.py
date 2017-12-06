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

    def __init__(self, untagged_data):
        """Instantiate Graph Updater."""
        self.untagged_data = untagged_data

    @classmethod
    def generate_and_update_packages(cls, apollo_temp_path):
        local_data_obj = LocalFileSystem(apollo_temp_path)
        file_list = local_data_obj.list_files()
        for file_name in file_list:
            data = local_data_obj.read_json_file(file_name)
            # TODO: use a singleton object with updated datafile.
            graph_obj = GraphUpdater(untagged_data=data)
            graph_obj.update_graph()
            local_data_obj.remove_json_file(file_name)

    def update_graph(self):
        """For each ecosystem, update the graph with packages that need tags."""

        for each_ecosystem in self.untagged_data:
            package_list = self.untagged_data[each_ecosystem]
            pck_len = len(package_list)
            if pck_len == 0:
                # If pck_len =0 then, no package of that ecosystem requires
                # tags. Hence, do nothing.
                continue
            for index in range(0, pck_len, 100):
                sub_package_list = package_list[index:index + 100]
                pay_load = self.generate_payload(
                    each_ecosystem, sub_package_list)
                self.execute_gremlin_dsl(pay_load)

    def generate_payload(self, ecosystem, package_list):
        """Generate the payload that is sent to graph.

            :param ecosystem: The ecosystem whose packages are to be updated.
            :param package_list: All the packages ina givne ecosystem that require tags.

            :return pay_load: The formated gremlin query."""

        # NOTE: This query assumes that the packages in the package_list are already
        # present in the graph.
        str_gremlin_query = "g.V().has('ecosystem', '{}')".format(ecosystem) + \
            ".has('name', within(str_packages))." + \
            "property('manual_tagging_required', true).valueMap();"
        pay_load = {
            'gremlin': str_gremlin_query,
            'bindings': {'str_packages': package_list}
        }
        return pay_load

    def execute_gremlin_dsl(self, pay_load):
        """Execute the gremlin query submitted in the pay load"""
        try:
            response = get_session_retry().post(
                GREMLIN_REST_URL, data=json.dumps(pay_load))
            if response.status_code != 200:
                _logger.error(
                    "The rest failed with payload::\n {}".format(pay_load))

        except Exception as e:
            _logger.error("Request failed with error:: \n {}".format(e))
