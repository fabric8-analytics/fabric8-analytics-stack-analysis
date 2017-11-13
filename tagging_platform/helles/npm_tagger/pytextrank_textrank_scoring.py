# -*- coding: UTF-8 -*-
import json
import pytextrank
import os
import mistune
from bs4 import BeautifulSoup
import daiquiri
import logging
from util.data_store import s3_data_store
from analytics_platform.kronos.src import config
import click


daiquiri.setup(level=logging.WARN)
_logger = daiquiri.getLogger(__name__)

stopwords = set([])
master_tag_list = set()

PATH_PREFIX = '/tmp/'
stage1_output_path = 'stage1_output'
final_output_path = 'output_tags'
tags_dict = {}


def getNPMdescription(package_name):
    """Get the NPM description for cases where a readme is missing"""
    npmjs_bucket = s3_data_store.S3DataStore(src_bucket_name='prod-repository-description',
                                             access_key=config.AWS_S3_ACCESS_KEY_ID,
                                             secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    matches = npmjs_bucket.list_files(prefix='npm/{}.txt'.format(package_name), max_count=1)
    if not matches:
        return False
    return npmjs_bucket.read_generic_file(matches[0])


def returnContentIfAscii(text):
    encoded = ''.join([i if ord(i) < 128 else ' ' for i in text]).strip()
    # If at least 80% of the content is ascii we can run tagging
    if len(encoded) >= 0.80 * len(text):
        return encoded
    else:
        return False


@click.command()
@click.option('--bucket-name',
              help='The bucket in which automated tags are stored',
              default='prod-bayesian-core-readme')
@click.option('--package-name',
              help='Particular package for which tags should be extracted, if required',
              default='')
def main(bucket_name, package_name):
    s3_bucket = s3_data_store.S3DataStore(src_bucket_name=bucket_name,
                                          access_key=config.AWS_S3_ACCESS_KEY_ID,
                                          secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    if not package_name:
        for readme_batch in s3_bucket.iterate_bucket_items():
            for idx, readme_filename in enumerate(readme_batch, 1):
                process_readme(idx, readme_filename, s3_bucket)
            break
        write_tag_batch_to_s3(tags_dict, single=False)
        print("Total packages tagged: {}".format(len(tags_dict)))
    else:
        process_readme(1, "npm/{}/README.json".format(package_name), s3_bucket)
        if tags_dict:
            write_tag_batch_to_s3(tags_dict, single=True)


def process_readme(idx, readme_filename, s3_bucket):
    if readme_filename.startswith('npm/'):
        package_name = readme_filename[len('npm/'):]
    if package_name.endswith('/README.json'):
        package_name = package_name[:-len('/README.json')]
    readme_content = s3_bucket.read_json_file(readme_filename)
    if not readme_content:
        npmjs_description = getNPMdescription(package_name)
        if not npmjs_description:
            _logger.warning("[MISSING_DATA] Readme/NPMJS description for package {} does "
                            "not exist in S3.".format(package_name))
            return
        else:
            readme_content = {
                'type': 'plaintext',
                'content': npmjs_description
            }
    if readme_content['type'] == 'Markdown' or readme_content['type'] == 'plaintext':
        readme_content['content'] = returnContentIfAscii(
            readme_content['content'].replace('\n', ' '))
        if not readme_content['content']:
            _logger.warning("[ENCODING] Ignoring package {} as the readme is not in"
                            " ascii".format(package_name))
            return
        if readme_content['type'] == 'Markdown':
            readme_content = markdown_preprocess(
                readme_content['content'])
        else:
            readme_content = readme_content['content']
        with open(os.path.join(PATH_PREFIX, package_name.replace('/', ':::')), 'w') as of:
            of.write(json.dumps({"id": idx, "text": readme_content}))
        curfilename = of.name
        of.close()
        try:
            tags = run_pipeline(curfilename)
            if tags:
                # print(tags)
                tags_dict[package_name] = tags
        except Exception:
            _logger.warning("[CONTENT] Could not get tags for {}".format(package_name))
        os.remove(curfilename)
    else:
        _logger.warning("[FORMAT] Skipping {}, content is not in markdown format"
                        " but in {}.".format(readme_filename, readme_content['type']))


def markdown_preprocess(markdown_content):
    readme_rendered = mistune.markdown(markdown_content, escape=False)
    soup = BeautifulSoup(readme_rendered, "html.parser")
    # Replace anchors with content where relevant and extract otherwise
    for link in soup.findAll('a'):
        if link.text.startswith('http'):
            link.extract()
        else:
            link.replaceWithChildren()
    # Remove all the images
    for image in soup.findAll('img'):
        image.extract()
    # Remove all the code blocks
    for code_block in soup.findAll('code'):
        code_block.extract()
    return soup.text


def load_stopwords():
    global stopwords
    with open("custom_stopwords.txt") as stopwords_file:
        stopwords = set(stopwords_file.read().strip().split('\n'))


def execute_stage_one(path_stage0):
    path_stage1 = path_stage0.split('::')[0] + ".stage1.output.dat"
    with open(os.path.join(PATH_PREFIX, path_stage1), 'w') as f:
        for graf in pytextrank.parse_doc(pytextrank.json_iter(
                                         os.path.join(PATH_PREFIX, path_stage0))):
            f.write("%s\n" % pytextrank.pretty_print(graf._asdict()))
    return path_stage1


def execute_stage_two(path_stage1):
    graph, ranks = pytextrank.text_rank(os.path.join(PATH_PREFIX, path_stage1))
    pytextrank.render_ranks(graph, ranks)
    path_name_components = path_stage1.split('.')
    path_name_components[path_name_components.index('stage1')] = 'stage2'
    path_stage2 = '-'.join(path_name_components)
    with open(os.path.join(PATH_PREFIX, path_stage2), 'w') as f:
        for rl in pytextrank.normalize_key_phrases(os.path.join(PATH_PREFIX, path_stage1),
                                                   ranks, stopwords=stopwords):
            f.write("%s\n" % pytextrank.pretty_print(rl._asdict()))
    return path_stage2


def get_key_phrases(path_stage2):
    phrases = set([p for p in pytextrank.limit_keyphrases(
        os.path.join(PATH_PREFIX, path_stage2), phrase_limit=3)])
    return phrases


def run_pipeline(stage0_filename):
    _logger.info("Running pipeline for: " + stage0_filename)
    stage1_filename = execute_stage_one(stage0_filename)
    stage2_filename = execute_stage_two(stage1_filename)
    if stage2_filename:
        os.remove(stage1_filename)
    tags = list(get_key_phrases(stage2_filename))
    os.remove(stage2_filename)
    return tags


def write_tag_batch_to_s3(tag_dict, single=False):
    tags_output_bucket = s3_data_store.S3DataStore(src_bucket_name='avgupta-stack-analysis-dev',
                                                   access_key=config.AWS_S3_ACCESS_KEY_ID,
                                                   secret_key=config.AWS_S3_SECRET_ACCESS_KEY)
    if not single:
        filename = 'package_tag_map'
    else:
        filename = list(tags_dict.keys())[0]
    tags_output_bucket.write_json_file('package_tag_maps/npm/{}.json'.format(filename),
                                       json.dumps(tag_dict))


if __name__ == '__main__':
    main()
