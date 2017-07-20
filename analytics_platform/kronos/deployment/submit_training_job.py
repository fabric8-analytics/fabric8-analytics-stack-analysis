# ----------------------------------------------------------------------------------------------------------------------
# This script spawns a spark emr cluster on AWS and submits a job to run the given src code.
#
# Dependency: It requires boto3 library.
#
# Reference:
#    http://stackoverflow.com/questions/36706512/how-do-you-automate-pyspark-jobs-on-emr-using-boto3-or-otherwise
#
# TODO:
# 1. Improve error handling
# ----------------------------------------------------------------------------------------------------------------------
import sys
import boto3
from time import gmtime, strftime
from analytics_platform.kronos.src import config

COMPONENT_PREFIX = "kronos"


def submit_job(input_bootstrap_file, input_src_code_file, training_data_url, fp_min_support_count,
               fp_intent_topic_count_threshold, fp_num_partition):
    str_cur_time = strftime("%Y_%m_%d_%H_%M_%S", gmtime())

    # S3 bucket/key, where the input spark job ( src code ) will be uploaded
    s3_bucket = config.DEPLOYMENT_PREFIX + '-automated-analytics-spark-jobs'
    s3_key = '{}_{}_spark_job.zip'.format(config.DEPLOYMENT_PREFIX, COMPONENT_PREFIX)
    s3_uri = 's3://{bucket}/{key}'.format(bucket=s3_bucket, key=s3_key)
    s3_bootstrap_key = '{}_bootstrap_action.sh'.format(COMPONENT_PREFIX)
    s3_bootstrap_uri = 's3://{bucket}/{key}'.format(bucket=s3_bucket, key=s3_bootstrap_key)

    # S3 bucket/key, where the spark job logs will be maintained
    # Note: these logs are AWS logs that tell us about application-id of YARN application
    #       we need to log into EMR cluster nodes and use application-id to view YARN logs
    s3_log_bucket = config.DEPLOYMENT_PREFIX + '-automated-analytics-spark-jobs'
    s3_log_key = '{}_{}_spark_emr_log_'.format(config.DEPLOYMENT_PREFIX, COMPONENT_PREFIX, str_cur_time)
    s3_log_uri = 's3://{bucket}/{key}'.format(bucket=s3_log_bucket, key=s3_log_key)

    print "Uploading the bootstrap action to AWS S3 URI " + s3_bootstrap_uri + " ..."
    # Note: This overwrites if file already exists
    s3_client = boto3.client('s3',
                             aws_access_key_id=config.AWS_S3_ACCESS_KEY_ID,
                             aws_secret_access_key=config.AWS_S3_SECRET_ACCESS_KEY)
    s3_client.upload_file(input_bootstrap_file, s3_bucket, s3_bootstrap_key)

    print "Uploading the src code to AWS S3 URI " + s3_uri + " ..."
    s3_client.upload_file(input_src_code_file, s3_bucket, s3_key)

    print "Starting spark emr cluster and submitting the jobs ..."
    emr_client = boto3.client('emr',
                              aws_access_key_id=config.AWS_S3_ACCESS_KEY_ID,
                              aws_secret_access_key=config.AWS_S3_SECRET_ACCESS_KEY,
                              region_name='us-east-1')
    response = emr_client.run_job_flow(
        Name=config.DEPLOYMENT_PREFIX + "_" + COMPONENT_PREFIX + "_" + str_cur_time,
        LogUri=s3_log_uri,
        ReleaseLabel='emr-5.2.1',
        Instances={
            # 'MasterInstanceType': 'm3.xlarge',
            # 'SlaveInstanceType': 'm3.xlarge',
            # 'InstanceCount': 1,
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
                                        "AWS_S3_SECRET_ACCESS_KEY": config.AWS_S3_SECRET_ACCESS_KEY,
                                        "AWS_GNOSIS_BUCKET": config.AWS_GNOSIS_BUCKET,
                                        "AWS_SOFTNET_BUCKET": config.AWS_SOFTNET_BUCKET,
                                        "AWS_KRONOS_BUCKET": config.AWS_KRONOS_BUCKET,
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
            # {
            #     'Name': 'Maximize Spark Default Config',
            #     'ScriptBootstrapAction': {
            #         'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config'
            #     }
            # },
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
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', s3_uri, '/home/hadoop/']
                }
            },
            {
                'Name': 'setup - unzip files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['unzip', '/home/hadoop/' + s3_key, '-d', '/home/hadoop']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit',
                             '--py-files',
                             '/home/hadoop/' + s3_key,
                             '/home/hadoop/analytics_platform/kronos/src/offline_training.py',
                             training_data_url,
                             fp_min_support_count,
                             fp_intent_topic_count_threshold,
                             fp_num_partition]
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
        output[
            'status_description'] = "The training is in progress. Please check the given training job after some time."
    else:
        output['training_job_id'] = "Error"
        output['status'] = 'Error'
        output['status_description'] = "Error! The job/cluster could not be created!"
        print response

    return output
