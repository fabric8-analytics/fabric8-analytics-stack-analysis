import logging
from analytics_platform.janus.src import config
from analytics_platform.janus.src.user_category import UserCategorizationModel
from util.data_store.spark_s3_data_store import SparkS3dataStore
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext


logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def train_janus():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    method = config.METHOD
    input_data_store = SparkS3dataStore(src_bucket_name=config.AWS_BUCKET,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY, sc=sc, sqlContext=sqlContext)
    assert (input_data_store is not None)

    output_data_store = SparkS3dataStore(src_bucket_name=config.AWS_TARGET_BUCKET,
                                    access_key=config.AWS_S3_ACCESS_KEY_ID,
                                    secret_key=config.AWS_S3_SECRET_ACCESS_KEY, sc=sc, sqlContext=sqlContext)
    assert (output_data_store is not None)

    model = UserCategorizationModel.train(input_data_store=input_data_store,
                                          min_k=int(config.MIN_K),
                                          max_k=int(config.MAX_K),
                                          method=method)

    file_name_prefix = "clustering_results/janus_" + method
    model.save(output_data_store, file_name_prefix)


if __name__ == '__main__':
    train_janus()
    