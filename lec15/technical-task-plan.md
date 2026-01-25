# План реалізації: Розподілений Spark на AWS EKS + Standalone Jupyter на EC2

Цей план описує перехід від локального `docker-compose` середовища до хмарної інфраструктури AWS, де Spark-кластер працюватиме в Kubernetes (EKS), а Jupyter — на окремому EC2 інстансі.

---

## Етап 1: Підготовка локального середовища
Перед початком необхідно встановити та налаштувати наступні інструменти (для macOS використовуємо Homebrew):

1.  **AWS CLI**: 
    *   Встановлення: `brew install awscli`
    *   Налаштування: `aws configure` (використовуйте свій IAM профіль)
    *   Переключення профілю: `export AWS_PROFILE=<YOUR_AWS_PROFILE>`
    *   **Необхідні IAM права (Permissions)**: 
        Рекомендовано використовувати `AdministratorAccess` для `eksctl`, оскільки він створює багато залежних ресурсів.

2.  **eksctl**: 
    *   Встановлення: `brew install eksctl`
3.  **kubectl**: 
    *   Встановлення: `brew install kubernetes-cli`
4.  **Helm**: 
    *   Встановлення: `brew install helm`

---

## Етап 2: Створення кластера AWS EKS
Для опису інфраструктури використовується файл `eks-cluster-config.yaml`. Він містить:
*   Регіон (наприклад, `eu-north-1`).
*   OIDC для IAM інтеграції.
*   Managed Node Group на Spot-інстансах (t3.small) для економії.
*   Обов'язкові політики для нод (`AmazonEKSWorkerNodePolicy`, `AmazonEC2ContainerRegistryReadOnly` тощо).

**Команда для створення кластера:**
```bash
eksctl create cluster -f eks-cluster-config.yaml
```

---

## Етап 3: Розгортання Spark кластера через Helm
Використовуємо чарт від Bitnami зі спеціальними налаштуваннями для роботи зовні кластера.

1.  **Додавання репозиторію:**
    ```bash
    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo update
    ```
2.  **Встановлення (Spark 3.5.x):**
    ```bash
    helm install spark-k8s bitnami/spark \
      --namespace spark-namespace --create-namespace \
      --version 9.3.3 \
      --set global.security.allowInsecureImages=true \
      --set image.registry=public.ecr.aws \
      --set image.repository=bitnami/spark \
      --set image.tag=3.5 \
      --set service.type=LoadBalancer \
      --set master.extraEnvVars[0].name=SPARK_PUBLIC_DNS \
      --set master.extraEnvVars[0].value=<AWS-LOAD-BALANCER-DNS>
    ```

---

## Етап 4: Налаштування Standalone Jupyter на окремому EC2

### Послідовність дій (CLI):
1. **Створення Security Group**:
   ```bash
   aws ec2 create-security-group --group-name jupyter-standalone-sg --vpc-id <VPC_ID>
   ```
2. **Відкриття портів**:
   *   22 (SSH) для доступу.
   *   8889 (Jupyter Лабораторія).
   *   4040 (Spark Application UI).
   *   Увесь трафік всередині VPC (`192.168.0.0/16`) для зв'язку з воркерами.

3. **Запуск інстансу**:
   Використовуйте `user-data` файл для автоматичного встановлення Docker та AWS CLI.

### Наступні кроки (після SSH на сервер):
1.  **Збірка образу**:
    ```bash
    cd ~/spark-base && docker build -t spark-image .
    ```
2.  **Запуск контейнера**:
    ```bash
    docker run -d -p 8889:8889 -p 4040:4040 \
      -e SPARK_MASTER=spark://<AWS-LOAD-BALANCER-DNS>:7077 \
      --name jupyter-standalone spark-image jupyter
    ```

---

## Етап 5: Робота з кодом (Best Practices)
Для успішного підключення до кластера з EC2, у Jupyter необхідно вказувати IP драйвера:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://<AWS-LOAD-BALANCER-DNS>:7077") \
    .appName("AWS-Cloud-Project") \
    .config("spark.driver.host", "<JUPYTER_PRIVATE_IP>") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.cores.max", "2") \
    .getOrCreate()
```

---

## Етап 6: Керування витратами (Cost Management)

1.  **Видалення на ніч**: EKS коштує ~$0.10/год навіть без навантаження.
    ```bash
    eksctl delete cluster -f eks-cluster-config.yaml
    ```
2.  **Інстанси**: Завжди використовуйте `Spot` для воркерів та `t3.small/medium` для тестів.

---

## Отримані уроки (Troubleshooting)
*   **ImagePullBackOff**: Завжди фіксуйте версію образу (image.tag) та використовуйте стабільні реєстри (public.ecr.aws).
*   **No Resources**: Якщо Spark каже, що немає ресурсів — перевірте ліміти пам'яті (`spark.executor.memory`) та "вбийте" старі додатки через Spark UI.
*   **Network**: Для повноцінної роботи Spark потрібен двосторонній зв'язок (Driver <-> Worker), тому `spark.driver.host` є обов'язковим.
