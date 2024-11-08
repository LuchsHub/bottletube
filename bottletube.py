#!/usr/bin/python3

import time
import os
import sys
import psycopg2

import requests
from botocore.exceptions import ClientError

from bottle import route, run, template, request, app
from boto3 import resource

import boto3

os.chdir(os.path.dirname(__file__))
sys.path.append(os.path.dirname(__file__))

application = app()


@route('/hello')
@route('/healthcheck')
def healthcheck():
    return requests.get('http://169.254.169.254/latest/meta-data/public-hostname').text


@route('/test')
def test():
    secret_name = "testSecret"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    return secret


@route('/home')
@route('/')
def home():
    # Read Entries from DB
    items = []
    cursor.execute('SELECT * FROM image_uploads ORDER BY id')
    for record in cursor.fetchall():
        items.append({'id': record[0],
                      'filename': record[1],
                      'category': record[2]})
    return template('home.tpl', name='BoTube Home', items=items,
                    BOTTLETUBE_CLOUDFRONT_DOMAIN_NAME=os.environ.get("BOTTLETUBE_CLOUDFRONT_DOMAIN_NAME"),
                    BOTTLETUBE_S3_BUCKET_ID=os.environ.get("BOTTLETUBE_S3_BUCKET_ID"))


@route('/upload', method='GET')
def do_upload_get():
    return template('upload.tpl', name='Upload Image',
                    BOTTLETUBE_CLOUDFRONT_DOMAIN_NAME=os.environ.get("BOTTLETUBE_CLOUDFRONT_DOMAIN_NAME"))


@route('/upload', method='POST')
def do_upload_post():
    category = request.forms.get('category')
    upload = request.files.get('file_upload')

    # Check for errors
    error_messages = []
    if not upload:
        error_messages.append('Please upload a file.')
    if not category:
        error_messages.append('Please enter a category.')

    try:
        name, ext = os.path.splitext(upload.filename)
        if ext not in ('.png', '.jpg', '.jpeg'):
            error_messages.append('File Type not allowed.')
    except:
        error_messages.append('Unknown error.')

    if error_messages:
        return template('upload.tpl', name='Upload Image', error_messages=error_messages)

    # Save to /tmp folder
    upload.filename = name + '_' + time.strftime("%Y%m%d-%H%M%S") + ext
    upload.save('images')

    # Upload to S3
    data = open('images/' + upload.filename, 'rb')
    s3_resource.Bucket(os.environ.get("BOTTLETUBE_S3_BUCKET_ID")).put_object(Key='user_uploads/' + upload.filename,
                                                                             Body=data,
                                                                             ACL='public-read')

    # Write to DB
    cursor.execute(
        f"INSERT INTO image_uploads (url, category) VALUES ('user_uploads/{upload.filename}', '{category}');")
    connection.commit()

    # Return template
    return template('upload_success.tpl', name='Upload Image',
                    BOTTLETUBE_CLOUDFRONT_DOMAIN_NAME=os.environ.get("BOTTLETUBE_CLOUDFRONT_DOMAIN_NAME"))


def get_secret_from_secrets_manager(secret_name):
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    return secret


# Connect to DB
rds_password = get_secret_from_secrets_manager("BOTTLETUBE_RDS_PASSWORD")
rds_host = os.environ.get("BOTTLETUBE_RDS_HOST")
connection = psycopg2.connect(user="postgres",
                              host=rds_host,
                              password=rds_password,
                              database="bottletube")
cursor = connection.cursor()
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS bottletube;
    SET SCHEMA 'bottletube';
    CREATE TABLE IF NOT EXISTS image_uploads
    (
        id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        url VARCHAR(256) NOT NULL,
        category varchar(64)
    );"""
               )
connection.commit()

# Connect to S3
s3_resource = resource('s3', region_name='us-east-1')

if __name__ == '__main__':
    # URL for the instance metadata service
    metadata_url = "http://169.254.169.254/latest/meta-data/"

    # Fetch the public DNS hostname
    response = requests.get(metadata_url + "public-hostname")
    ec2_public_dns_hostname = response.text

    run(host=ec2_public_dns_hostname, port=8080)
