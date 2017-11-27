import util.softnet_util as utils
import daiquiri
import logging
import time

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


class CooccurrenceMatrixGenerator(object):
    """Cooccurrence Matrix Generator: Responsible for generating cooccurence
    matrix required for Kronos Training."""

    def __init__(self, matrix_dict):
        """Instantiate Cooccurrence Matrix Generator."""
        # TODO matrix_df

        self._matrix_dict = matrix_dict

    @classmethod
    def generate_cooccurrence_matrix(cls, kronos_dependency_dict, list_of_package_list):

        kronos_intent_dependency_dict = kronos_dependency_dict.get(
            utils.KD_INTENT_DEPENDENCY_MAP)
        kronos_component_dependency_dict = kronos_dependency_dict.get(
            utils. KD_COMPONENT_DEPENDENCY_MAP)
        node_list = kronos_dependency_dict.get(utils.KD_PACKAGE_LIST) + \
            kronos_dependency_dict.get(utils.KD_INTENT_LIST)

        cooccurrence_matrix = cls._generate_cooccurrence_matrix_for_ecosystem(
            list_of_package_list=list_of_package_list, node_list=node_list,
            kronos_intent_dependency_dict=kronos_intent_dependency_dict,
            kronos_component_dependency_dict=kronos_component_dependency_dict)

        return CooccurrenceMatrixGenerator(cooccurrence_matrix)

    def save(self, data_store, filename):
        data_store.write_pandas_df_into_json_file(
            data=self._matrix_dict, filename=filename)

    @classmethod
    def load(cls, data_store, filename):
        cooccurrence_matrix = data_store.read_json_file_into_pandas_df(filename)
        return CooccurrenceMatrixGenerator(cooccurrence_matrix)

    def get_matrix_dictionary(self):
        return self._matrix_dict

    @classmethod
    def get_component_class_occurrence(cls, row_component_package_dict):
        component_class_occurrence = 0
        for value in row_component_package_dict.values():
            if value == 1:
                component_class_occurrence = 1
                break
        return component_class_occurrence

    @classmethod
    def get_intent_occurrence(cls, row_intent_component_dict):
        intent_occurrence = 0
        den = len(row_intent_component_dict.values())
        num = sum(row_intent_component_dict.values())
        value = float(num) / float(den)
        if value > 0.4:
            intent_occurrence = 1
        return intent_occurrence

    @classmethod
    def _generate_cooccurrence_matrix_for_ecosystem(cls, list_of_package_list, node_list,
                                                    kronos_component_dependency_dict,
                                                    kronos_intent_dependency_dict):
        _logger.info("Co-occurence matrix generation for ecosystem started")
        row_count = len(list_of_package_list)
        cooccurrence_matrix = utils.create_empty_pandas_df(
            rowsize=row_count, columns_list=node_list)
        component_class_list = kronos_component_dependency_dict.keys()

        for row_id in range(0, row_count):
            package_list = list_of_package_list[row_id]
            package_list = [x.lower() for x in package_list]

            for package in package_list:
                cooccurrence_matrix.loc[[row_id], package] = 1

            for component_class in component_class_list:
                component_package_list = kronos_component_dependency_dict[
                    component_class]
                row_component_package_df = cooccurrence_matrix.loc[[row_id],
                                                                   component_package_list]
                row_component_package_dict = row_component_package_df.to_dict(
                    orient="index")[row_id]
                component_class_occurrence = cls.get_component_class_occurrence(
                    row_component_package_dict)
                cooccurrence_matrix.loc[
                    [row_id], component_class] = component_class_occurrence

            temp_node_list = list(component_class_list)
            intent_list = kronos_intent_dependency_dict.keys()

            for intent in intent_list:
                children_intent_list = kronos_intent_dependency_dict[intent]
                if set(children_intent_list) < set(temp_node_list):
                    row_intent_component_df = cooccurrence_matrix.loc[[row_id],
                                                                      children_intent_list]
                    row_intent_component_dict = row_intent_component_df.to_dict(
                        orient="index")[row_id]
                    intent_occurrence = cls.get_intent_occurrence(
                        row_intent_component_dict)
                    cooccurrence_matrix.loc[
                        [row_id], intent] = intent_occurrence
                    temp_node_list.append(intent)
                else:
                    intent_list.append(intent)
        _logger.info("Complete Co-occurence matrix generated")
        t0 = time.time()
        sampled = cooccurrence_matrix.sample(frac=utils.COM_SAMPLE_RATIO)
        _logger.info("Sampling took: {}".format(time.time() - t0))
        return sampled
