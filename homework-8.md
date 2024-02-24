# Домашнее задание №8

## Ссылки

- [Ссылка на манифесты](https://github.com/uncle-alfer/otus-mlops-homework-project/tree/main/k8s)
- [Ссылка на Dockerfile с приложением](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/Dockerfile)
- [Ссылка на api](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/src/prediction/main.py)


## Что было сделано

- написан сервис для сервинга модели (src/main.py)
- созданы k8s-манифесты для деплоя
- поднят кластер (Managed Service for Kubernetes)
- собран [docker-образ и запушен в регистри](https://hub.docker.com/repository/docker/unclealfer/mlops-hw8/general)
- этот образ передан в качесте образа для deployment
- для демонстрации были прокинуты порты из кластера на локальную машину

Также был создан GitHub Action для коммитов и пул реквестов в ветку deploy.

[Ссылка на пайплайн](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/.github/workflows/docker-image.yml)

[Ссылка на успешный запуск билда](https://github.com/uncle-alfer/otus-mlops-homework-project/actions/runs/8031015146/job/21938924694?pr=6)

## Скрины с демонстрацией работы

![Кластер](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/5abab023-9914-4855-b052-1bb69e1240c1)
![Прокинутые порты](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/8b52f8ba-b1ad-47e1-b85d-673c20719396)
![Ответ сервиса](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/73fcc9e5-0f18-4928-95af-a7cddf0a3672)
