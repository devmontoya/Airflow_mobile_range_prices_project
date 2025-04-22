import sys
from airflow.decorators import dag, task
from datetime import datetime

PATH_BASE = '/opt/airflow/dags/'
sys.path.append(PATH_BASE)

from common.email_management import success_email, failure_email, default_args

@task.virtualenv(
    task_id="preprocess_data", 
        requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn==1.5.2",
        "joblib",
        "feature-engine==1.8.2"
        ], 
    system_site_packages=False
)
def preprocess_data():
    import sys
    PATH_BASE = '/opt/airflow/dags/'
    sys.path.append(PATH_BASE)

    import pandas as pd
    from joblib import load
    
    # Se carga pipeline de preprocesado (crea y remueve features, aplica minmax basado en el conjunto de entrenamiento, entre otros)
    preprocess_pp = load(PATH_BASE +'pipelines/prange_preproce_pipeline.joblib')

    # Se carga conjunto de datos sobre los cuales se realizará la predicción y se ejecuta el pipeline de preprocesado sobre él
    PATH_DATASETS = PATH_BASE+'data/inputs'+'/'
    X_input = pd.read_csv(PATH_DATASETS+'X_input.csv')

    X_input_pp = pd.DataFrame(preprocess_pp.transform(X_input))

    PATH_INTERMEDIATE_DATASETS = PATH_BASE+'data/intermediate'+'/'
    X_input_pp.to_csv(PATH_INTERMEDIATE_DATASETS+'X_input_pp.csv', index=False)
    

@task.virtualenv(
    task_id="prediction_task", 
    requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn==1.5.2",
        "joblib",
        "feature-engine==1.8.2"
        ], 
    system_site_packages=False
)
def prediction_task():
    """Realiza el Split para el conjunto de datos, crea un pipeline de preprocesado de datos y pipeline de entrenamiento"""
    import sys
    PATH_BASE = '/opt/airflow/dags/'
    sys.path.append(PATH_BASE)

    import pandas as pd
    
    from joblib import load

    # Se carga pipeline encargado de aplicar el modelo de ml previamente entrenado
    prange_model_pp = load(PATH_BASE +'pipelines/prange_model_pipeline.joblib')

    PATH_INPUT_DATASETS = PATH_BASE+'data/inputs'+'/'
    PATH_INTERMEDIATE_DATASETS = PATH_BASE+'data/intermediate'+'/'
    PATH_OUTPUT = PATH_BASE+'data/outputs'+'/'

    X_input = pd.read_csv(PATH_INPUT_DATASETS+'X_input.csv')
    X_input_pp = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'X_input_pp.csv')


    prediction = prange_model_pp.predict(X_input_pp.values)

    X_input['price_range_prediction'] = prediction
    mapping = {0:'Low', 1:'Mid', 2:'High', 3:'Very High'}
    X_input['price_range_prediction'] = X_input['price_range_prediction'].map(mapping)

    X_input.to_csv(PATH_OUTPUT+'Prediction.csv', index=False)

@dag(
    dag_id='prediction_dag',
    default_args = default_args,
    start_date=datetime(2025, 4, 1),
    on_failure_callback = lambda context: failure_email(context),
    on_success_callback = lambda context: success_email(context),
    schedule=None
)
def prediction_dag():
    preprocess_data_tk = preprocess_data()
    prediction_tk = prediction_task()

    preprocess_data_tk >> prediction_tk

prediction_dag()
