from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth

from analytics_platform.gnosis.src.abstract_gnosis import AbstractGnosis
from analytics_platform.gnosis.src.gnosis_package_topic_model import GnosisPackageTopicModel
from gnosis_constants import *
from gnosis_util import *



class GnosisReferenceArchitecture(AbstractGnosis):
    """
    GnosisReferenceArchitecture i.e. Reference Architecture Generator
    """

    def __init__(self, dictionary):
        """
        Instantiates Gnosis Reference Architecture dictionary

        :param gnosis_model: the gnosis model
        """
        self._dictionary = dictionary

    @classmethod
    def train(cls, data_store, min_support_count=FP_MIN_SUPPORT_COUNT,
              min_intent_topic_count=FP_INTENT_TOPIC_COUNT_THRESHOLD):

        """
        Generates the Gnosis Reference Architecture

        :param data_store: input data store containing the processed package topic map and list of manifest files
        :param min_support_count: minimum support count to be used by FP Growth Algo
        :param min_intent_topic_count: minimum number of allowed topics per intent
        :return: the Gnosis Reference Architecture dictionary
        """


        gnosis_ptm_obj = GnosisPackageTopicModel.load(data_store=data_store,
                                                      filename=GNOSIS_PTM_OUTPUT_PATH)
        eco_to_package_topic_dict = gnosis_ptm_obj.get_dictionary()

        eco_to_package_to_topic_dict = eco_to_package_topic_dict[GNOSIS_PTM_PACKAGE_TOPIC_MAP]

        fp_growth_model = cls._train_fp_growth_model(data_store=data_store,
                                                     eco_to_package_topic_dict=eco_to_package_to_topic_dict,
                                                     min_support_count=min_support_count)

        gnosis_intent_to_component_class_dict = cls._generate_intent_component_class_dict_fp_growth(
            model=fp_growth_model, min_intent_topic_count=min_intent_topic_count)

        gnosis_component_class_list = cls._generate_component_class_list_for_eco_package_topic_dict(
            eco_to_package_topic_dict=eco_to_package_to_topic_dict)

        # TODO: modify this while implementing multiple levels in the reference architecture
        gnosis_intent_to_intent_dict = {}

        gnosis_intent_list = cls._generate_intent_list(
            gnosis_intent_to_intent_dict=gnosis_intent_to_intent_dict,
            gnosis_intent_to_component_class_dict=gnosis_intent_to_component_class_dict)

        gnosis_edge_list = cls._generate_edge_list(
            gnosis_intent_to_component_class_dict=gnosis_intent_to_component_class_dict,
            gnosis_intent_to_intent_dict=gnosis_intent_to_intent_dict)

        gnosis_model = cls._generate_gnosis_model(
            gnosis_intent_to_intent_dict=gnosis_intent_to_intent_dict,
            gnosis_intent_to_component_class_dict=gnosis_intent_to_component_class_dict,
            gnosis_component_class_list=gnosis_component_class_list,
            gnosis_intent_list=gnosis_intent_list,
            gnosis_edge_list=gnosis_edge_list)

        return gnosis_model

    @classmethod
    def load(cls, data_store, filename):
        """
        Loads already saved Gnosis

        :param data_store: Data store to load Gnosis from
        :param filename: the file from which Gnosis is to be loaded from
        :return: a Gnosis object
        """

        gnosis_ra_json = data_store.read_json_file(filename=filename)
        gnosis_ra_dict = dict(gnosis_ra_json)
        gnosis_ra_obj = GnosisReferenceArchitecture(dictionary=gnosis_ra_dict)
        return gnosis_ra_obj

    def save(self, data_store, filename):
        """
        Saves the Gnosis object in json format

        :param data_store: Data store to save Gnosis in
        :param filename: the file into which Gnosis is to be saved
        :return: None
        """

        data_store.write_json_file(filename=filename, contents=self.get_dictionary())
        return None

    def get_dictionary(self):
        return self._dictionary

    @classmethod
    def _generate_component_class_list(clas, gnosis_intent_component_class_dict):
        """
        Generates the component class list

        :param gnosis_intent_component_class_dict: intent-component_class dict
        :return: the list of component classes
        """
        component_class_list = generate_value_list_from_dict(dictionary=gnosis_intent_component_class_dict)
        return component_class_list

    @classmethod
    def _generate_edge_list(cls, gnosis_intent_to_component_class_dict, gnosis_intent_to_intent_dict):
        """
        Generates the list of edges as the list of tuples  [(source,destination),(source,destination),...]

        :param gnosis_intent_to_component_class_dict: lowest level of Gnosis hierarchy in dict format
        :param gnosis_intent_to_intent_dict: all the levels except the lowest level of Gnosis hierarchy in dict format
        :return: list of edges where edges are represented as tuples
        """
        intent_to_component_class_edge_list = generate_key_to_value_edges(
            dictionary=gnosis_intent_to_component_class_dict)
        intent_to_intent_edge_list = generate_key_to_value_edges(dictionary=gnosis_intent_to_intent_dict)
        edge_list = intent_to_component_class_edge_list + intent_to_intent_edge_list
        return edge_list

    @classmethod
    def _generate_intent_list(cls, gnosis_intent_to_intent_dict, gnosis_intent_to_component_class_dict):
        """
        Generates the list of intents

        :param gnosis_intent_to_intent_dict: all the levels except the lowest level of Gnosis hierarchy in dict format
        :return: list of intents
        """
        super_intent_list = gnosis_intent_to_intent_dict.keys()
        sub_intent_list = generate_value_list_from_dict(gnosis_intent_to_intent_dict)
        intent_list = gnosis_intent_to_component_class_dict.keys()
        node_set = set.union(set(super_intent_list), set(sub_intent_list), set(intent_list))
        node_list = list(node_set)
        return node_list

    @classmethod
    def _generate_gnosis_model(cls, gnosis_intent_to_intent_dict,
                               gnosis_intent_to_component_class_dict, gnosis_component_class_list, gnosis_intent_list,
                               gnosis_edge_list):
        """
        Generates the Gnosis model

        :param gnosis_edge_list_string: list of edges in the string format '[(source,destination),(source,destination),...]'
        :param gnosis_intent_to_intent_dict: Intent to Intent map
        :param gnosis_intent_component_class_dict: Intent to component class map
        :param gnosis_component_class_list: Component class list
        :param gnosis_intent_list: Intent list
        :return: Gnosis model
        """
        gnosis_ra_dict = dict()
        gnosis_ra_dict[GNOSIS_RA_DICT] = dict(gnosis_intent_to_intent_dict, **gnosis_intent_to_component_class_dict)
        gnosis_ra_dict[GNOSIS_RA_COMPONENT_CLASS_LIST] = gnosis_component_class_list
        gnosis_ra_dict[GNOSIS_RA_INTENT_LIST] = gnosis_intent_list
        gnosis_ra_dict[GNOSIS_RA_EDGE_LIST] = gnosis_edge_list

        gnosis_ra_obj = GnosisReferenceArchitecture(dictionary=gnosis_ra_dict)
        return gnosis_ra_obj

    @classmethod
    def _train_fp_growth_model(cls, data_store, eco_to_package_topic_dict, min_support_count):
        sc = SparkContext()
        manifest_file_list = data_store.list_files(prefix=MANIFEST_FILEPATH)
        list_of_topic_list = list()
        for manifest_file in manifest_file_list:
            eco_to_package_list_json_array = data_store.read_json_file(manifest_file)
            for eco_to_package_list_json in eco_to_package_list_json_array:
                ecosystem = eco_to_package_list_json.get(MANIFEST_ECOSYSTEM)
                list_of_package_list = eco_to_package_list_json.get(MANIFEST_PACKAGE_LIST)
                for package_list in list_of_package_list:
                    topic_list = cls.get_topic_list_for_package_list(package_list, ecosystem, eco_to_package_topic_dict)
                    list_of_topic_list.append(topic_list)
        transactions = sc.parallelize(list_of_topic_list)
        transactions.cache()

        min_support = float(min_support_count / float(transactions.count()))

        model = FPGrowth.train(transactions, minSupport=min_support,
                               numPartitions=FP_NUM_PARTITION)

        return model

    @classmethod
    def _generate_intent_component_class_dict_fp_growth(cls, model, min_intent_topic_count):
        result = model.freqItemsets().collect()
        gnosis_intent_component_class_dict = dict()
        for fi in result:
            if len(fi.items) > min_intent_topic_count:
                gnosis_intent_component_class_dict["".join(fi.items)] = fi.items
        return gnosis_intent_component_class_dict

    @classmethod
    def _generate_component_class_list_for_eco_package_topic_dict(cls, eco_to_package_topic_dict):
        # TODO raise exception when ecosystem is not there in eco_to_package_topic_dict
        gnosis_component_class_list = list()

        for ecosystem in eco_to_package_topic_dict:
            component_class_list = cls._generate_component_class_list(
                gnosis_intent_component_class_dict=eco_to_package_topic_dict[ecosystem])
            gnosis_component_class_list.extend(component_class_list)
        gnosis_component_class_list = list(set(gnosis_component_class_list))

        return gnosis_component_class_list

    @classmethod
    def get_topic_list_for_package_list(cls, package_list, ecosystem, eco_to_package_topic_dict):
        # TODO raise exception when package or ecosystem is not there in eco_to_package_topic_dict
        topic_set = set()
        for package in package_list:
            topic_set |= (set(eco_to_package_topic_dict[ecosystem][package]))
        return list(topic_set)
