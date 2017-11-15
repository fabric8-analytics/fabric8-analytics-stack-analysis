from util.data_store.s3_data_store import S3DataStore
from analytics_platform.kronos.src import config
import json
import os
import daiquiri
import logging

daiquiri.setup(level=logging.WARN)
logger = daiquiri.getLogger(__name__)


def run(ecosystem='npm', bucket_name='prod-bayesian-core-data',
        input_data_path=''):
    if not input_data_path:
        logger.warning("No data path given, not proceeding further.")
        return
    core_data = S3DataStore(src_bucket_name=bucket_name,
                            access_key=config.AWS_S3_ACCESS_KEY_ID,
                            secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    input_bucket_name, key = input_data_path.split('/', 2)[-1].split('/', 1)
    input_bucket = S3DataStore(src_bucket_name=input_bucket_name,
                               access_key=config.AWS_S3_ACCESS_KEY_ID,
                               secret_key=config.AWS_S3_SECRET_ACCESS_KEY)

    package_tag_map = json.loads(input_bucket.read_json_file(key))
    package_list = list(package_tag_map.keys())
    descriptions = {}

    for package in package_list:
        version_folders = sorted(core_data.list_folders(
            prefix=os.path.join(ecosystem, package)), reverse=True)
        if not version_folders:
            logger.warning("No data exists for {}".format(package))
        latest_version = version_folders[0]
        try:
            meta = core_data.read_json_file(
                os.path.join(latest_version, 'metadata.json'))
        except Exception:
            logger.warning(
                'No metadata exists in S3 for the package: {}'.format(package))
            continue
        if 'details' in meta and len(meta['details']) > 0:
            descriptions[package] = meta['details'][0].get(
                'description', '')
        else:
            descriptions[package] = ''
        print(descriptions)

    input_bucket.write_json_file(
        'tagging/npm/missing_data/missing_data.json', json.dumps(descriptions))


if __name__ == '__main__':
    run(input_data_path='s3://avgupta-stack-analysis-dev/package_tag_maps/'
        'npm/package_tag_map.json')
