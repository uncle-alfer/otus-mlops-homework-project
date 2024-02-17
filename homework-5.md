# Домашнее задание №5

Имя бакета: s3://mlops-hw3-vos
[Ссылка на бакет](https://mlops-hw3-vos.website.yandexcloud.net)

Папка в бакете для mlflow: s3://mlops-hw3-vos/mlflow/artifacts

[Ссылка на папку с airflow содержимым](https://github.com/uncle-alfer/otus-mlops-homework-project/tree/main/airflow)

[Ссылка на DAG](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/airflow/dags/cleanse_data_dag.py)

Схема DAG'а:

- по SFTP передается скрипт по очистке на кластер
- по SSH запускается команда по копированию сырых данных из s3 в hdfs
- по SSH запускается команда spark-submit

PS. в целях экономии времени и ресурсов один из запусков (второй) был ручным. Первый и последнйи были по расписанию.

Код со скриптом по очистке можно посмотреть по [ссылке](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/airflow/utils/cleanse_data.py).

Скрины, демонстрирующие рабочий DAG:
![4-1](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/20bd5a7d-8646-4825-bb7d-a9156e238f1c)

![4-2](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/168de5a5-70ee-42b2-84cf-b94f23ab96c2)

![4-3](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/5a47cee7-03bd-4dfc-b710-632f828c0a86)

Очищенные датасеты, сохраненные по итогам работы скрипта в кластере (data_cleansed_{datetime.now()}.parquet):
![изображение](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/c8a53180-06c1-433a-816f-6f925a8d2e54)
