import logging
from analytics_platform.janus.src import config
from analytics_platform.janus.src.user_category import UserCategorizationModel
from util.data_store.spark_s3_data_store import SparkS3dataStore
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext


logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def bulk_score_janus():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    method = config.METHOD
    input_data_store = SparkS3dataStore(src_bucket_name=config.AWS_BUCKET,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY, sc=sc, sqlContext=sqlContext)
    assert (input_data_store is not None)

    pipeline_store = SparkS3dataStore(src_bucket_name=config.AWS_TARGET_BUCKET,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY, sc=sc, sqlContext=sqlContext)
    assert (pipeline_store is not None)
    
    batch_score_config_values = { 'src_bucket_name':config.AWS_BATCH_BUCKET,
                'access_key': config.AWS_S3_ACCESS_KEY_ID,
                'secret_key': config.AWS_S3_SECRET_ACCESS_KEY

    }
    broadcast_config = sc.broadcast(batch_score_config_values)
    
    file_name_prefix = "clustering_results/janus_" + method
    model_loaded = UserCategorizationModel.load(pipeline_store, file_name_prefix)
    model_loaded.batch_score(input_data_store, broadcast_config)


if __name__ == '__main__':
    bulk_score_janus()
