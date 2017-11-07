import json
import pytextrank
import os
import mistune
from bs4 import BeautifulSoup
import daiquiri
import logging

daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger(__name__)

stopwords = set([])
master_tag_list = set()

PATH_PREFIX = 'sample_data/'


def load_stopwords():
    global stopwords
    with open("custom_stopwords.txt") as stopwords_file:
        stopwords = set(stopwords_file.read().strip().split('\n'))


def execute_stage_one(path_stage0):
    path_stage1 = path_stage0.split('::')[0] + ".stage1.output.dat"
    with open(os.path.join(PATH_PREFIX, path_stage1), 'w') as f:
        for graf in pytextrank.parse_doc(pytextrank.json_iter(os.path.join(PATH_PREFIX, path_stage0))):
            f.write("%s\n" % pytextrank.pretty_print(graf._asdict()))
    return path_stage1


def execute_stage_two(path_stage1):
    graph, ranks = pytextrank.text_rank(os.path.join(PATH_PREFIX, path_stage1))
    pytextrank.render_ranks(graph, ranks)
    path_name_components = path_stage1.split('.')
    path_name_components[path_name_components.index('stage1')] = 'stage2'
    path_stage2 = '-'.join(path_name_components)
    with open(os.path.join(PATH_PREFIX, path_stage2), 'w') as f:
        for rl in pytextrank.normalize_key_phrases(os.path.join(PATH_PREFIX, path_stage1), ranks, stopwords=stopwords):
            f.write("%s\n" % pytextrank.pretty_print(rl._asdict()))
            # to view output in this notebook
            # print(pytextrank.pretty_print(rl))
    return path_stage2


def get_key_phrases(path_stage2):
    phrases = set([p for p in pytextrank.limit_keyphrases(os.path.join(PATH_PREFIX, path_stage2), phrase_limit=4)])
    print("**keywords:** %s" % (phrases))
    return phrases


def clean_and_store_readme_sections(npm_data_dict):
    for idx, package_name in enumerate(npm_data_dict.keys(), 1):
        readme = npm_data_dict[package_name]['readme']
        readme_rendered = mistune.markdown(readme, escape=False)
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
        with open(os.path.join(PATH_PREFIX, '{}::{}.json'.format(package_name.replace('/', ':'), idx)), 'w') as cleaned_readme_store:
            cleaned_readme_store.write(
                json.dumps({"id": idx, "text": soup.text}))


def run_pipeline(stage0_filename):
    logger.info("Running pipeline for: " + stage0_filename)
    stage1_filename = execute_stage_one(stage0_filename)
    stage2_filename = execute_stage_two(stage1_filename)
    return list(get_key_phrases(stage2_filename))


def main():
    global master_tag_list
    load_stopwords()
    with open('npm_data.json') as npm_data:
        npm_json = json.loads(npm_data.read())
    clean_and_store_readme_sections(npm_json)
    keyphrase_list = {}
    for json_file in os.listdir(PATH_PREFIX):
        try:
            package_full_name = json_file.split('::')[0].replace(":", "/")
            keyphrase_list.setdefault(package_full_name, run_pipeline(json_file))
            master_tag_list = master_tag_list.union(set(keyphrase_list.get(package_full_name, [])))
        except Exception as e:
            logger.warning("Could not tag:" + json_file)
            logger.debug(e)
    with open(os.path.join(PATH_PREFIX, 'final_result.json'), 'w') as final_result_outfile:
        final_result_outfile.write(json.dumps(keyphrase_list))
    with open(os.path.join(PATH_PREFIX, 'master_tag_list.json'), 'w') as master_tag_list_file:
        master_tag_list_file.write(json.dumps(list(master_tag_list)))

if __name__ == '__main__':
    main()
