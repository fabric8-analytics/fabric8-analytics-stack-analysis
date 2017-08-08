import json

from analytics_platform.janus.src import config
import logging
from util.data_store.local_filesystem import LocalFileSystem
from util.data_store.s3_data_store import S3DataStore
from analytics_platform.janus.src.user_category import UserCategorizationModel

logging.basicConfig(filename=config.LOGFILE_PATH, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def test_user_categorization_training():
    input_data_store = S3DataStore(src_bucket_name=config.AWS_BUCKET,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    assert (input_data_store is not None)

    output_data_store = LocalFileSystem(src_dir='/tmp/janus')
    # output_data_store = S3DataStore(src_bucket_name=config.AWS_TARGET_BUCKET,
    #                                 access_key=config.AWS_S3_ACCESS_KEY_ID,
    #                                 secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    assert (output_data_store is not None)

    model = UserCategorizationModel.train(input_data_store=input_data_store,
                                          min_k=int(config.MIN_K),
                                          max_k=int(config.MAX_K),
                                          num_files=int(config.NUM_FILES))
    assert (model is not None)

    model.save(output_data_store, "janus")


def test_user_categorization_scoring():
    input_data_store = S3DataStore(src_bucket_name=config.AWS_TARGET_BUCKET,
                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    assert (input_data_store is not None)

    model = UserCategorizationModel.load(data_store=input_data_store, file_name_prefix="janus")
    assert (model is not None)

    with open('data/user1_metadata.json', 'r') as f:
        data = json.load(f)
    score = model.score(data)
    assert (score is not None)

