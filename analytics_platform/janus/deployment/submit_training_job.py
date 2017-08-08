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
from analytics_platform.janus.src import config


COMPONENT_PREFIX = "janus"


def submit_job(input_bootstrap_file, input_src_code_file):
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
        Name=config.DEPLOYMENT_PREFIX + "_janus_" + str_cur_time,
        LogUri=s3_log_uri,
        ReleaseLabel='emr-5.2.1',
        Instances={
            # 'MasterInstanceType': 'm3.xlarge',
            # 'SlaveInstanceType': 'm3.xlarge',
            # 'InstanceCount': 1,
            'KeepJobFlowAliveWhenNoSteps': False, #
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-50271f16',
            'Ec2KeyName': 'Zeppelin2Spark',
            'InstanceGroups': [
                {
                    'Name': '{}_master_group'.format(COMPONENT_PREFIX),
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm3.xlarge',
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
                                        "AWS_BUCKET": config.AWS_BUCKET,
                                        "AWS_TARGET_BUCKET": config.AWS_TARGET_BUCKET,
                                        "AWS_BATCH_BUCKET": config.AWS_BATCH_BUCKET,
                                        "MIN_K": config.MIN_K,
                                        "MAX_K": config.MAX_K,
                                        "NUM_FILES": config.NUM_FILES,
                                        "METHOD": config.METHOD,
                                        "DEPLOYMENT_PREFIX": config.DEPLOYMENT_PREFIX
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
                'Args': ['unzip', '/home/hadoop/' + s3_key, '-d',  '/home/hadoop']
            }
        },
        {
            'Name': 'Run Spark',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--py-files', '/home/hadoop/' + s3_key, '/home/hadoop/analytics_platform/janus/src/training.py']
            }
        },
        {
            'Name': 'Run Spark',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--py-files', '/home/hadoop/' + s3_key, '/home/hadoop/analytics_platform/janus/src/batch_score.py']
            }
        }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )

    output = {}
    if response.get('ResponseMetadata').get('HTTPStatusCode') == 200:
        output['status'] = 'Success'
        output['message'] = "Done! The cluster was submitted successfully! Job flow id is " + response.get('JobFlowId')
    else:
        output['status'] = 'Error'
        output['message'] = "Error! The job/cluster could not be created!"
        print response

    return output


if __name__ == "__main__":
    # Gather input arguments
    if len(sys.argv) < 3:
        usage = sys.argv[0] + " <bootstrap_file> <src_code_file> \n"
        example = sys.argv[0] + " bootstrap_action.sh gen_ref_stacks.py \n"
        print("Error: Insufficient arguments!")
        print("Usage: " + usage)
        print("Example: " + example)
        sys.exit(1)

    bootstrap_file = sys.argv[1]
    src_code_file = sys.argv[2]
    report = submit_job(bootstrap_file, src_code_file)
    print report.get('status')
    print report.get('message')
