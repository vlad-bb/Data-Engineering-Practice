# 📋 Чек-лист розгортання Data Platform

Цей документ допоможе вам розгорнути стек інфраструктури, враховуючи збереження даних між сесіями.

## 🟢 Крок 1: Налаштування AWS CLI
1. Оберіть потрібний профіль та регіон:
   ```bash
   export AWS_PROFILE=data_engineer_vlad
   export AWS_DEFAULT_REGION=eu-north-1
   ```
2. Перевірте статус: `aws sts get-caller-identity`

## 🟢 Крок 2: Підготовка параметрів (.env)
Переконайтеся, що файл `.env` містить актуальні паролі та унікальну назву бакета для Airflow:
```bash
export $(grep -v '^#' .env | xargs)
```

## 🟢 Крок 3: Перевірка на конфлікти (Resource Existence)
Якщо ви вже розгортали цей стек раніше і видаляли його, деякі ресурси могли залишитися (`DeletionPolicy: Retain`).
*   **S3 Data Lake**: `aws s3 ls` (перевірте наявність бакета `data-platform-data-lake-...`)
*   **Glue Database**: `aws glue get-database --name data-platform_database`

### ⚠️ Якщо ресурси ВЖЕ існують (Імпорт):
Замість звичайного деплою, використовуйте процедуру імпорту, щоб не втратити дані:
1. Використовуйте `import_resources_to_stack.sh` для створення Change Set типу `IMPORT`.
2. Виконайте його:
   ```bash
   aws cloudformation execute-change-set --change-set-name ImportExistingData --stack-name data-platform-production --region eu-north-1
   ```
3. Тільки після цього запускайте повний деплой (Крок 4).

## 🟢 Крок 4: Повне розгортання (Deployment)
Якщо стек новий або ви вже зробили імпорт існуючих ресурсів:
```bash
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
```

## 🟢 Крок 5: Доступ до Airflow
1. Отримайте URL:
   ```bash
   aws cloudformation describe-stacks --stack-name data-platform-production --query "Stacks[0].Outputs[?OutputKey=='AirflowWebUI'].OutputValue" --output text
   ```
2. **Логін**: `admin`
3. **Пароль**: значення `$AirflowAdminPassword` з вашого `.env`.

## 🟢 Крок 6: Важливі нюанси шаблону
*   **Glue Crawlers**: Шляхи до S3 обов'язково повинні починатися з `s3://` (наприклад, `!Sub 's3://${DataLakeBucket}/raw/'`).
*   **Redshift Connection**: База даних за замовчуванням — `dev`.
*   **Retention**: Ресурси S3 та Glue Database мають `DeletionPolicy: Retain`. Вони НЕ видаляються автоматично разом зі стеком.

---
⚠️ **Економія коштів**: В кінці робочого дня видаляйте стек, щоб зупинити нарахування за NAT Gateway, RDS та Redshift:
```bash
aws cloudformation delete-stack --stack-name data-platform-production --region eu-north-1
```
Дані в S3 та метадані в Glue залишаться для наступного сеансу.
