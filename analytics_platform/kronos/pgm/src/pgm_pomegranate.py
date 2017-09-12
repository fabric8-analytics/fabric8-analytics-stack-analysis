from pomegranate import BayesianNetwork

from analytics_platform.kronos.pgm.src.abstract_pgm import AbstractPGM
from analytics_platform.kronos.pgm.src.pgm_constants import *
from analytics_platform.kronos.pgm.src.pgm_util import *
from util.data_store.local_filesystem import LocalFileSystem
from util.data_store.s3_data_store import S3DataStore
import pickle
from joblib import Parallel, delayed
import functools

class PGMPomegranate(AbstractPGM):
    """Kronos - The Knowledge Graph."""

    def __init__(self, model):
        self._model = model

    @property
    def model(self):
        return self._model

    @classmethod
    def train(cls, kronos_dependency_dict, package_occurrence_df):
        kronos_model = cls._train_kronos_for_ecosystem(
            kronos_dependency_dict=kronos_dependency_dict,
            package_occurrence_df=package_occurrence_df)

        return PGMPomegranate(kronos_model)

    def save(self, data_store, filename):
        pgm_model = self.model
        if type(data_store) is LocalFileSystem:
            data_store.write_pomegranate_model(model=pgm_model, filename=filename)
        if type(data_store) is S3DataStore:
            local_filename = "/tmp/kronos.json"
            with open(local_filename, 'wb') as f:
                pickle.dump(pgm_model.to_json(), f, pickle.HIGHEST_PROTOCOL)
            data_store.upload_file(local_filename, filename)
        return None

    @classmethod
    def load(cls, data_store, filename):
        pgm_model = None
        if type(data_store) is LocalFileSystem:
            pgm_model = data_store.read_pomegranate_model(filename=filename)
        if type(data_store) is S3DataStore:
            local_filename = "/tmp/kronos.json"
            data_store.download_file(filename, local_filename)
            with open(local_filename, 'rb') as f:
                pgm_model = BayesianNetwork.from_json(pickle.load(f))
        return PGMPomegranate(pgm_model)

    @classmethod
    def _train_kronos_for_ecosystem(cls, kronos_dependency_dict, package_occurrence_df):

        kronos_dependency_list_string = kronos_dependency_dict[KD_PARENT_TUPLE_LIST]
        kronos_node_list = kronos_dependency_dict[KD_PACKAGE_LIST] + \
            kronos_dependency_dict[KD_INTENT_LIST]
        kronos_node_string_list = [node_name.decode('utf-8')
                                   if type(node_name) == bytes else node_name
                                   for node_name in kronos_node_list]
        kronos_dependency_list = generate_kronos_dependency_list_for_pomegranate(
            kronos_dependency_list_string)

        package_occurrence_matrix = generate_matrix_from_pandas_df(package_occurrence_df,
                                                                   kronos_node_list)

        pgm_model = BayesianNetwork.from_structure(package_occurrence_matrix,
                                                   structure=kronos_dependency_list,
                                                   state_names=kronos_node_string_list)
        return pgm_model

    def score(self, evidence_dict_list):
        global pgm_model_kronos
        pgm_model_kronos = self.model
        n_jobs = len(evidence_dict_list)
        result_array = functools.reduce(
            list.__add__,
            Parallel(n_jobs=n_jobs)(
                delayed(parallel_predict)([evidence_dict_list[i]]) for i in range(n_jobs))
        )
        return result_array


def parallel_predict(X):
    return list(map(pgm_model_kronos.predict_proba, X, [3] * len(X)))
