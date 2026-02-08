export $(grep -v '^#' .env | xargs)
aws cloudformation deploy \
  --template-file DataPlatform.yml \
  --stack-name data-platform-production \
  --region eu-north-1 \
  --parameter-overrides \
    ProjectName=data-platform \
    RedshiftMasterPassword=$RedshiftMasterPassword \
    AirflowAdminPassword=$AirflowAdminPassword \
    AirflowS3BucketName=$AirflowS3BucketName \
  --capabilities CAPABILITY_NAMED_IAM