# Airflow mobile range prices project

En este proyecto se ejemplifica el uso de **Dags** en **Airflow** para automatizar el entrenamiento y la predicción dada por un modelo sencillo de Machine learning. Este modelo en específico busca encontrar el rango de precio de teléfonos celulares, ver [mobile-price-classification - Kaggle](https://www.kaggle.com/datasets/iabhishekofficial/mobile-price-classification/) donde se propone este problema y conjunto de datos. Además se hace uso de un servicio **SMTP** a través de gmail para permitir el envío de notificaciones una vez un **Dag** termine.

### Dataset:
https://www.kaggle.com/datasets/iabhishekofficial/mobile-price-classification/data

### Modo de uso:

- `docker compose up` Iniciar los contenedores involucrados
- Ingresar a http://localhost:8080/
- `docker compose down --volumes` Remover los contenedores, incluyendo los volumes creados
