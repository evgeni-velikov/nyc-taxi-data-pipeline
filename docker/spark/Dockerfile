FROM bitnami/spark:3.5

USER root

RUN pip install boto3 s3fs

# S3 config defaults
ENV AWS_ACCESS_KEY_ID=minio
ENV AWS_SECRET_ACCESS_KEY=minio123
ENV AWS_ENDPOINT_URL=http://minio:9000
