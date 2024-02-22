# Домашнее задание №7

## Ссылки

Имя бакета: s3://mlops-hw3-vos
[Ссылка на бакет](https://mlops-hw3-vos.website.yandexcloud.net)

[Ссылка на вспомогательный ноутбук c дз](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/notebooks/hw7.ipynb)

[Ссылка на spark-consumer](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/notebooks/consumer.py)

[Ссылка на producer](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/notebooks/producer.py)

## Шаги

- скрипт producer.py каждые 10 секунд пишет в топик clicks json-данные
- скрипт consumer.py через spark streaming читает топик clicks
- скрипт consumer.py получает датафрейм
- скрипт consumer.py делает предсказание
- скрипт consumer.py записывает результат (добавляя к исходному json ключ 'prediction') в топик results

## Скрин с демонстрацией работы:

Левый верхний терминал - producer.py
Левый нижний терминал - просто демонстрация итоговой записи (читатель топика results)
Правый терминал - consumer.py

![hw7](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/f7f73c23-6827-4aa3-9f84-0f49922cb9f4)
