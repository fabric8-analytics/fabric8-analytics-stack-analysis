from util.data_store.s3_data_store import S3DataStore
from analytics_platform.kronos.src import config
import json
import os
import daiquiri
import logging

daiquiri.setup(level=logging.WARN)
logger = daiquiri.getLogger(__name__)


def run_job(input_data_path=''):
    if not input_data_path:
        logger.warning("No input data path given, gracefully exiting.")
        return
    input_bucket_name, key = input_data_path.split('/', 2)[-1].split('/', 1)
    input_bucket = S3DataStore(src_bucket_name=input_bucket_name,
                               access_key=config.AWS_S3_ACCESS_KEY_ID,
                               secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    core_data = S3DataStore(src_bucket_name='prod-bayesian-core-data',
                            access_key=config.AWS_S3_ACCESS_KEY_ID,
                            secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    packages = input_bucket.read_json_file(key)
    count = 1

    version_info = {}

    for package in packages:
        versions = sorted(core_data.list_folders(prefix=os.path.join('npm', package)), reverse=True)
        if not versions:
            logger.warning("[MISSING_DATA] Do not have data for any "
                           "versions of package {}".format(package))
            continue
        try:
            version = versions[0].split('/')[2]
        except Exception:
            logger.warning("[KEY_FORMAT] Could not get version for {}".format(package))
        version_info[package] = version
        print(count)
        count += 1
    input_bucket.write_json_file('s3://avgupta-stack-analysis-dev/tagging/npm/missing_data/'
                                 'missing_version_info.json', json.dumps(version_info))


if __name__ == '__main__':
    run_job(input_data_path='s3://avgupta-stack-analysis-dev/missing_data.json')
