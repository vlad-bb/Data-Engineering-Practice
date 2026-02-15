# 1. Завантажуємо паролі
export $(grep -v '^#' .env | xargs)
export AWS_PROFILE=data_engineer_vlad
export AWS_DEFAULT_REGION=eu-north-1

# 2. Створюємо Change Set для імпорту (тільки S3)
aws cloudformation create-change-set \
  --stack-name data-platform-production \
  --change-set-name ImportExistingData \
  --change-set-type IMPORT \
  --template-body file://ImportTemplate.yml \
  --parameters \
    ParameterKey=ProjectName,ParameterValue=data-platform \
  --resources-to-import file://resources-to-import.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --region eu-north-1