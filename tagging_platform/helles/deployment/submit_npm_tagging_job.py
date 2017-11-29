"""
This script spawns a spark emr cluster on AWS and submits a job to run the given src code.
"""
import logging
import boto3
from time import gmtime, strftime
import daiquiri

from analytics_platform.kronos.src import config

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)

COMPONENT_PREFIX = "helles"


def submit_tagging_job(input_bootstrap_file, input_src_code_file, package_name='',
                       manifest_path=''):
    str_cur_time = strftime("%Y_%m_%d_%H_%M_%S", gmtime())

    # S3 bucket/key, where the input spark job ( src code ) will be uploaded
    s3_bucket = config.DEPLOYMENT_PREFIX + '-automated-analytics-spark-jobs'
    s3_key = '{}_{}_spark_job.zip'.format(
        config.DEPLOYMENT_PREFIX, COMPONENT_PREFIX)
    s3_uri = 's3://{bucket}/{key}'.format(bucket=s3_bucket, key=s3_key)
    s3_bootstrap_key = '{}_bootstrap_action.sh'.format(COMPONENT_PREFIX)
    s3_bootstrap_uri = 's3://{bucket}/{key}'.format(
        bucket=s3_bucket, key=s3_bootstrap_key)

    # S3 bucket/key, where the spark job logs will be maintained
    # Note: these logs are AWS logs that tell us about application-id of YARN application
    #       we need to log into EMR cluster nodes and use application-id to view YARN logs
    s3_log_bucket = config.DEPLOYMENT_PREFIX + '-automated-analytics-spark-jobs'
    s3_log_key = '{}_{}_spark_emr_log_{}'.format(config.DEPLOYMENT_PREFIX, COMPONENT_PREFIX,
                                                 str_cur_time)
    s3_log_uri = 's3://{bucket}/{key}'.format(
        bucket=s3_log_bucket, key=s3_log_key)

    _logger.debug("Uploading the bootstrap action to AWS S3 URI {} ...".format(s3_bootstrap_uri))
    # Note: This overwrites if file already exists
    s3_client = boto3.client('s3',
                             aws_access_key_id=config.AWS_S3_ACCESS_KEY_ID,
                             aws_secret_access_key=config.AWS_S3_SECRET_ACCESS_KEY)
    s3_client.upload_file(input_bootstrap_file, s3_bucket, s3_bootstrap_key)

    _logger.debug("Uploading the bootstrap action to AWS S3 URI {} ...".format(s3_bootstrap_uri))
    s3_client.upload_file(input_src_code_file, s3_bucket, s3_key)

    _logger.debug("Starting spark emr cluster and submitting the jobs ...")
    emr_client = boto3.client('emr',
                              aws_access_key_id=config.AWS_S3_ACCESS_KEY_ID,
                              aws_secret_access_key=config.AWS_S3_SECRET_ACCESS_KEY,
                              region_name='us-east-1')
    script_name = '/home/hadoop/tagging_platform/helles/npm_tagger/pytextrank_textrank_scoring.py'
    args = ['/usr/bin/python3', script_name]
    if package_name:
        args = args + ['--package-name', package_name]
    elif manifest_path:
        args = args + ['--manifest-path', manifest_path]
    response = emr_client.run_job_flow(
        Name=config.DEPLOYMENT_PREFIX + "_" + COMPONENT_PREFIX + "_" + str_cur_time,
        LogUri=s3_log_uri,
        ReleaseLabel='emr-5.2.1',
        Instances={
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-50271f16',
            'Ec2KeyName': 'Zeppelin2Spark',
            'InstanceGroups': [
                {
                    'Name': '{}_master_group'.format(COMPONENT_PREFIX),
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'r4.2xlarge',
                    'InstanceCount': 1,
                    'Configurations': [
                        {
                            "Classification": "spark-env",
                            "Properties": {},
                            "Configurations": [
                                {
                                    "Classification": "export",
                                    "Configurations": [],
                                    "Properties": {
                                        "LC_ALL": "en_US.UTF-8",
                                        "LANG": "en_US.UTF-8",
                                        "AWS_S3_ACCESS_KEY_ID": config.AWS_S3_ACCESS_KEY_ID,
                                        "AWS_S3_SECRET_ACCESS_KEY": config.AWS_S3_SECRET_ACCESS_KEY
                                    }
                                }
                            ]
                        }
                    ]
                }
            ],
        },
        Applications=[
            {
                'Name': 'Spark'
            }
        ],
        BootstrapActions=[
            {
                'Name': 'Install Dependencies',
                'ScriptBootstrapAction': {
                    'Path': s3_bootstrap_uri
                }
            }
        ],
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'setup - copy files',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', s3_uri, '/home/hadoop/']
                }
            },
            {
                'Name': 'setup - unzip files',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['unzip', '/home/hadoop/' + s3_key, '-d', '/home/hadoop']
                }
            },
            {
                'Name': 'Run a tagging job',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': args
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )

    output = {}
    if response.get('ResponseMetadata').get('HTTPStatusCode') == 200:

        output['training_job_id'] = response.get('JobFlowId')
        output['status'] = 'work_in_progress'
        output['status_description'] = ("The tagging is in progress. Please check the given "
                                        "tagging job after some time.")
    else:
        output['training_job_id'] = "Error"
        output['status'] = 'Error'
        output['status_description'] = "Error! The job/cluster could not be created!"
        _logger.debug(response)

    return output
