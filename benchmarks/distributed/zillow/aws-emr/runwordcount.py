#!/usr/bin/env python3
# this runs the AWS provided example for serverless EMR, adopted to fit Lambda setting.

import boto3
import subprocess
import shlex
import json
import logging
import time

def run_command(cmd):
    args = shlex.split(cmd)
    p = subprocess.run(args, capture_output=True)

    # parse in, check whether ret code is wrong -> throw exception
    if 0 != p.returncode:
        raise Exception('Process failed: {}'.format(p.stderr.decode()))

    # return stdout
    return p.stdout.decode()


def run_awscli(cmd):
    ret = run_command(cmd)
    if len(ret) > 0:
        return json.loads(ret)
    else:
        return {}

# global config
EMR_APPLICATION_NAME='emrWordCount'
EMR_ROLE='emrExecutionRole'
EMR_BUCKET='serverless-emr'
EMR_ACCESS_POLICY='emrS3AccessPolicy'

def ensure_s3_bucket(s3_client, bucket_name, region):
    bucket_names = list(map(lambda b: b['Name'], s3_client.list_buckets()['Buckets']))

    if bucket_name not in bucket_names:
        logging.info('Bucket {} not found, creating (private bucket) in {} ...'.format(bucket_name, region))

        # bug in boto3:
        if region == current_region():
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info('Bucket {} created in {}'.format(bucket_name, region))
        else:
            location = {'LocationConstraint': region.strip()}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
            logging.info('Bucket {} created in {}'.format(bucket_name, region))
    else:
        logging.info('Found bucket {}'.format(bucket_name))


def create_emr_role(iam_client, role_name, bucket_name, access_policy_name, region):
    trust_policy = '{"Version":"2012-10-17","Statement":[{"Sid":"EMRServerlessTrustPolicy","Action":"sts:AssumeRole","Effect":"Allow","Principal":{"Service":"emr-serverless.amazonaws.com"}}]}'

    access_policy_document = '{"Version":"2012-10-17","Statement":[{"Sid":"ReadAccessForEMRSamples","Effect":"Allow","Action":["s3:GetObject","s3:ListBucket"],"Resource":["arn:aws:s3:::*.elasticmapreduce","arn:aws:s3:::*.elasticmapreduce/*"]},{"Sid":"FullAccessToOutputBucket","Effect":"Allow","Action":["s3:PutObject","s3:GetObject","s3:ListBucket","s3:DeleteObject"],"Resource":["arn:aws:s3:::serverless-emr","arn:aws:s3:::serverless-emr/*"]}]}'
    access_policy_document = access_policy_document.replace('serverless-emr', EMR_BUCKET)

    response = iam_client.create_role(RoleName=role_name,
                                      AssumeRolePolicyDocument=trust_policy,
                                      Description='auto-created role for Serverless EMR')
    role_arn = response['Role']['Arn']
    logging.info('Created role {} with ARN {}'.format(role_name, role_arn))

    response = iam_client.put_role_policy(RoleName=role_name,
                                          PolicyName=access_policy_name,
                                          PolicyDocument=access_policy_document)
    logging.info('Put role policy {} in place granting access to S3 bucket {}'.format(access_policy_name, bucket_name))

def remove_emr_role(iam_client, role_name):
    policy_names = iam_client.list_role_policies(RoleName=role_name)['PolicyNames']

    for name in policy_names:
        try:
            iam_client.delete_role_policy(RoleName=role_name, PolicyName=name)
        except Exception as e:
            logging.error('Error while detaching policy {}, EMR setup corrupted? Details: {}'.format(name, e))

    # delete role...
    iam_client.delete_role(RoleName=role_name)

def setup_emr_role(iam_client, role_name, bucket_name, access_policy_name, region, overwrite):
    try:
        response = iam_client.get_role(RoleName=role_name)
        logging.info('Found EMR role from {}'.format(response['Role']['CreateDate']))

        # throw dummy exception to force overwrite
        if overwrite:
            remove_emr_role(iam_client, role_name)
            logging.info('Overwriting existing role {}'.format(role_name))
            create_emr_role(iam_client, role_name, bucket_name, access_policy_name, region)

    except iam_client.exceptions.NoSuchEntityException as e:
        logging.info('Role {} was not found in {}, creating ...'.format(role_name, region))
        create_emr_role(iam_client, role_name, bucket_name, access_policy_name, region)

def create_application(application_name):
    create_cmd = "aws emr-serverless create-application --release-label emr-6.5.0-preview --type 'SPARK' --name '{}'".format(application_name)

    ret = run_awscli(create_cmd)
    logging.info('Created EMR Serverless Spark application {} with id {}'.format(ret['name'], ret['applicationId']))
    logging.info('Application ARN: {}'.format(ret['arn']))

def get_application_id(application_name):
    list_cmd = "aws emr-serverless list-applications"
    ret = run_awscli(list_cmd)
    applications = ret['applications']
    for app in applications:
        if app['name'] == application_name:
            return app['id']
    return None

def remove_application(application_name):

    id = get_application_id(application_name)
    if id is None:
        raise Exception('can not find application {}'.format(application_name))

    try:
        stop_cmd = "aws emr-serverless stop-application --application-id {}".format(id)
        ret = run_awscli(stop_cmd)
        logging.info('stopped application')
    except:
        pass
    del_cmd = "aws emr-serverless delete-application --application-id {}".format(id)
    ret = run_awscli(del_cmd)
    logging.info('deleted application {}'.format(application_name))

def setup_application(application_name, overwrite):
    # check if application exists
    if get_application_id(application_name) is not None:
        if overwrite:
            remove_application(application_name)

            create_application(application_name)
        else:
            raise Exception('Cannot create application {}, already exists.'.format(application_name))
    else:
        create_application(application_name)

def setup_emr(application_name, role_name, bucket_name, access_policy_name, region='us-east-1', overwrite=True):
    iam_client = boto3.client('iam')
    s3_client = boto3.client('s3')

    ensure_s3_bucket(s3_client, bucket_name, region)

    setup_emr_role(iam_client, role_name, bucket_name, access_policy_name, region, overwrite)

    setup_application(application_name, overwrite)

def wait_for_state(application_name, id, state, sleep_interval=0.1):

    # wait till application is created
    wait_cmd = "aws emr-serverless get-application --application-id {}".format(id)
    ret = run_awscli(wait_cmd)
    while ret['application']['state'] != state:
        logging.info('Application {} is in state {}, waiting for state {}...'.format(application_name,
                                                                                          ret['application']['state'],
                                                                                     state))
        time.sleep(0.1)
        ret = run_awscli(wait_cmd)

def start_emr(application_name):

    id = get_application_id(application_name)
    if id is None:
        raise Exception('could not find application {}'.format(application_name))

    wait_for_state(application_name, id, 'CREATED')
    logging.info('Starting application {}'.format(application_name))
    ts = time.time()
    start_cmd = "aws emr-serverless start-application --application-id {}".format(id)
    ret = run_awscli(start_cmd)
    wait_for_state(application_name, id, 'STARTED')
    logging.info('Took {}s to start application {}'.format(time.time() - ts, application_name))

def main():
    logging.info('Running wordcount AWS EMR benchmark')

    # setting up role and application if they don't exist
    logging.info('Running EMR setup')
    setup_emr(EMR_APPLICATION_NAME, EMR_ROLE, EMR_BUCKET, EMR_ACCESS_POLICY)
    logging.info('EMR setup done')

    # starting application & invoking job!
    start_emr(EMR_APPLICATION_NAME)
    # stopping application & removing everything?

if __name__ == '__main__':

    # set up logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(levelname)s: %(message)s',
                        handlers=[logging.FileHandler("experiment.log", mode='w'),
                                  logging.StreamHandler()])
    stream_handler = [h for h in logging.root.handlers if isinstance(h, logging.StreamHandler)][0]
    stream_handler.setLevel(logging.INFO)

    main()