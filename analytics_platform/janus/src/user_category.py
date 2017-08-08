from __future__ import division, print_function
import sys
import json
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import *
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline, PipelineModel
from analytics_platform.janus.src import config
from analytics_platform.janus.src.create_model import model_kmeans 
from util.error import error_codes
from util.error.analytics_exception import AnalyticsException
from util.data_store.spark_s3_data_store import SparkS3dataStore


def _get_raw_dataframe(load_data=None):  
    input_data = load_data.map(lambda x: json.loads(x[1])).map(lambda y:(y.get('company', 'unknown').lower(), y.get('email', 'unknown').split('@')[-1]))
    schema = StructType([StructField("company", StringType(), True), StructField("email", StringType(), True)])
    raw_df = input_data.toDF(schema)
    return raw_df


def _get_string_indexer(indexed_col=[]):  
    stringIndexer = list()
    for i in indexed_col:
        stringIndexer.append(StringIndexer(inputCol=i, outputCol="indexded_"+i).setHandleInvalid("skip"))
        return stringIndexer


def _get_feature_assembler(raw_df=None, feature_list=[]):
    assembler = VectorAssembler(inputCols=[x for x in raw_df.columns if x in feature_list], outputCol='features')
    return assembler


def _get_pre_process_transformers(raw_df=None, indexed_col=[], feature_list=[]):
    string_indexer = _get_string_indexer(indexed_col)
    assembler = _get_feature_assembler(raw_df, feature_list)
    transformers = string_indexer + [assembler]
    return transformers


def _get_clean_data(raw_df=None, transformers=[]):
    pre_process_pipeline = Pipeline(stages=transformers)
    clean_df = pre_process_pipeline.fit(raw_df).transform(raw_df)
    return clean_df

def _get_result_dict(d):
    d_tosave = {
        'company':d['company'],
        'email_group':d['email'],
        'user_group': str(d['prediction'] + 1)
    }
    return d_tosave


def _get_json(row_val, broadcast_config):
    src_bucket_name = broadcast_config.value['src_bucket_name']
    access_key = broadcast_config.value['access_key']
    secret_key = broadcast_config.value['secret_key']
    output_data_store = SparkS3dataStore(src_bucket_name, access_key, secret_key)
    d = row_val.asDict()
    d_tosave = _get_result_dict(d)
    prefix = d['email'] + "_" + d_tosave['user_group']
    file_name = "result_json/" + str(d_tosave['user_group']) + "/" + prefix
    output_data_store.write_json_file(file_name, contents=d_tosave)
    

class UserCategorizationModel(object):
    """
    User categorization model
    """

    def __init__(self):
        """
        Instantiate User Categorization Model object.
        """
        user_persona_pipeline = None
        # label_encoder = None


    # TODO: check how existing usage data ( in S3 ) looks like
    @classmethod
    def train(cls, input_data_store, min_k, max_k,method="KMEANS"):        
        """
        Train a user categorization model from the data available in the given data source.

        The input data can be considered a huge collection of transactions where each
        transaction denotes a specific Bayesian Analytics request by a specific user.

        Each user transaction should be available in the following json format:
        {
            user: {
                type: '',
                account: {
                    id: ''
                }
            },
            timestamp: '',
            manifest_files: ['', '']
        }

        :param input_data_store: Data store to read input data from
        :return: None
        """
        load_data = input_data_store.read_all_json_files(prefix="raw_json/")
        raw_df = _get_raw_dataframe(load_data)
        indexed_col = ['company', 'email']
        feature_list = ['indexed_company', 'indexed_email']
        transformers = _get_pre_process_transformers(raw_df, indexed_col, feature_list)
        clean_df = _get_clean_data(raw_df, transformers)
        model = UserCategorizationModel()
        if method.upper() == "KMEANS":
            model.user_persona_pipeline = model_kmeans(raw_df, clean_df, transformers, min_k, max_k)    
        return model


    def save(self, data_store, file_name_prefix):
        """
        Save the User Categorization Model.

        :param data_store: Data store to keep the model.
        :param file_name_prefix: Prefix for files that will contain model.
        """
        data_store.save_tos3_pipeline(self.user_persona_pipeline, file_name_prefix)


    @classmethod
    def load(cls, data_store, file_name_prefix):
        """
        Load the User Categorization Model.

        :param data_store: Data store to keep the model.
        :param file_name_prefix: Prefix for files that contain model.
        """
        model = UserCategorizationModel()
        model.user_persona_pipeline = data_store.load_froms3_pipeline(file_name_prefix)
        return model

    def score(self, load_data):
        """
        Identifies the category of the user in the given transactions where each transaction
        denotes a specific Bayesian Analytics request by a specific user.

        Each user transaction should be in the following json format:
        {
            user: {
                type: '',
                account: {
                    id: ''
                }
            },
            timestamp: '',
            manifest_files: ['', '']
        }

        :param list_transactions: List of transactions where user category needs to be identified
        :return: list of transactions with the identified user category
        """
        # Make sure model is either trained/loaded already
        if self.user_persona_pipeline is None: # or self.label_encoder is None:
            raise AnalyticsException(error_codes.ERR_MODEL_NOT_AVAILABLE)

        response_data = {}
        raw_df = _get_raw_dataframe(load_data)
        result = self.user_persona_pipeline.transform(raw_df).take(1)
        if len(result):
            result = result[0].asDict()
            response_data = _get_result_dict(result)
        return response_data

    def batch_score(self, input_data_store, broadcast_config):
        if self.user_persona_pipeline is None:
            raise AnalyticsException(error_codes.ERR_MODEL_NOT_AVAILABLE)

        load_data = input_data_store.read_all_json_files(prefix="raw_json/")
        raw_df = _get_raw_dataframe(load_data)
        results = self.user_persona_pipeline.transform(raw_df)
        results.foreach(lambda x: _get_json(x, broadcast_config))
