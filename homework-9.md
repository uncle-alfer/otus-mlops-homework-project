# Домашнее задание №9

## Ссылки

- [Ссылка на манифесты](https://github.com/uncle-alfer/otus-mlops-homework-project/tree/main/k8s)
- [Ссылка на Dockerfile с приложением](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/Dockerfile)
- [Ссылка на api](https://github.com/uncle-alfer/otus-mlops-homework-project/blob/main/src/prediction/main.py)


## Что было сделано

- поднят кластер (Managed Service for Kubernetes)
- в приложение были добавлены метрики Prometheus (искусственные, фактически счетчики ответов)
- было задеплоено приложение (predictor)
- была задеплоена система мониторинга - стек Prometheus+Grafana (helm install monitoring prometheus-community/kube-prometheus-stack)
- был задеплоен ServiceMonitor для сервиса predictor-service
- для демонстрации были прокинуты порты из кластера на локальную машину (сам сервис, grafana, prometheus)
- В Grafana был добавлен дашборд для метрики "alert_counter"
- В Grafana был добавлен алерт на событие "alert_counter>5"

При вызове сервиса со значением terminal_id=13 счетчик alert_counter сбрасывается.

## Скрины с демонстрацией работы

Активный ServiceMonitor в Prometheus Targets:
![изображение](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/612c94c4-15b2-455d-b8c1-b90fd9890946)

Дашборд с метрикой alert_counter
![изображение](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/6e743857-a5a3-4887-9bf5-a1e7c35e0dab)

Правило для алерта
![изображение](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/5ea1bc3d-09fa-454f-a8b9-3b2b1b602f05)

![изображение](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/50970ec8-fd9a-43ff-8f6a-078e28442b8f)

Сработавший алерт
![изображение](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/27afb606-d50d-43f0-b5a3-7bf508374023)

Сброс счетчика (вызов сервиса со значением terminal_id=13)
![изображение](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/58240a01-07da-469a-a947-bc395c6f4c46)

Нормальизация алерта
![изображение](https://github.com/uncle-alfer/otus-mlops-homework-project/assets/70284100/54a0995f-7ff9-4f58-bb6c-41051e58c033)
