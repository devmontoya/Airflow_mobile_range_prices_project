import sys
from datetime import datetime
from airflow.decorators import dag, task

PATH_BASE = '/opt/airflow/dags/'
sys.path.append(PATH_BASE)

from common.email_management import success_email, failure_email, default_args


@task.virtualenv(
    task_id="split_training_data", 
    requirements=["pandas", "scikit-learn==1.5.2"], 
    system_site_packages=False
)
def split_training_data():
    import sys
    PATH_BASE = '/opt/airflow/dags/'
    sys.path.append(PATH_BASE)

    from sklearn.model_selection import train_test_split

    import pandas as pd

    # Carga de archivo de entrenamiento
    df = pd.read_csv(PATH_BASE+'data/inputs/data_mobile_price_range.csv')

    X_train, X_test, y_train, y_test = train_test_split(
        df.drop(['price_range'], axis=1), 
        df['price_range'], 
        test_size=0.1, 
        random_state=0,
    )

    PATH_INTERMEDIATE_DATASETS = PATH_BASE+'data/intermediate'+'/'
    X_train.to_csv(PATH_INTERMEDIATE_DATASETS+'X_train.csv', index=False)
    X_test.to_csv(PATH_INTERMEDIATE_DATASETS+'X_test.csv', index=False)
    y_train.to_csv(PATH_INTERMEDIATE_DATASETS+'y_train.csv', index=False)
    y_test.to_csv(PATH_INTERMEDIATE_DATASETS+'y_test.csv', index=False)

    print(f'Datasets escritos en {PATH_INTERMEDIATE_DATASETS}')

@task.virtualenv(
    task_id="preprocessing_pp",
    requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn==1.5.2",
        "joblib",
        "feature-engine==1.8.2"
        ], 
    system_site_packages=False
)
def preprocessing_pp():
    """Realiza el Split para el conjunto de datos, crea un pipeline de preprocesado de datos y pipeline de entrenamiento"""
    import sys
    PATH_BASE = '/opt/airflow/dags/'
    sys.path.append(PATH_BASE)

    import pandas as pd
    
    from feature_engine.selection import DropFeatures

    import joblib

    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import MinMaxScaler

    import common.preprocessors as pp

    # Se cargan datasets para entrenar el modelo
    PATH_INTERMEDIATE_DATASETS = PATH_BASE+'data/intermediate'+'/'
    X_train = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'X_train.csv')
    y_train = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'y_train.csv')

    X_test = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'X_test.csv')
    y_test = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'y_test.csv')

    # Se crea pipeline de preprocesado

    features_non_zero = ['battery_power', 'clock_speed', 'int_memory', 'm_dep',
       'mobile_wt', 'n_cores', 'px_height', 'px_width', 'ram', 'sc_h',
       'sc_w', 'talk_time']
    
    train_set = pd.concat([X_train, y_train], axis=1) # Se concatenan temporalmente
    train_set = train_set[(train_set[features_non_zero] > 0).all(axis=1)]
    train_set = train_set[(train_set['px_width']/train_set['px_height']) <= 20]
    X_train = train_set.drop(['price_range'], axis=1)
    y_train = train_set['price_range']
    prange_pp_pipeline = Pipeline(
        [('screen_features', pp.FeatureCreator()),
        ('drop_used_features', DropFeatures(features_to_drop=['px_width','px_height','sc_w', 'sc_h'])),
        ('drop_features_less_important', DropFeatures(features_to_drop=['wifi', 'touch_screen', 'four_g', 'dual_sim', 'blue', 'three_g'])),
        ('scaler', MinMaxScaler()),
        ]
    )

    prange_pp_pipeline.fit(X_train)

    # Se guarda pipeline de preprocesado
    joblib.dump(prange_pp_pipeline, PATH_BASE +'pipelines/prange_preproce_pipeline.joblib')

    # Pipeline aplicado sobre el conjunto de entrenamiento
    X_train = pd.DataFrame(
        prange_pp_pipeline.transform(X_train)
    )

    # Pipeline aplicado sobre el conjunto de prueba, notar que es el mismo usado anteriormente
    X_test = pd.DataFrame(
        prange_pp_pipeline.transform(X_test)
    )

    new_columns = ['battery_power', 'clock_speed', 'fc', 'int_memory', 'm_dep',
       'mobile_wt', 'n_cores', 'pc', 'ram', 'talk_time', 'num_pix',
       'aspect_ratio']
    
    # Se añade nuevamente las columnas
    #X_train.columns = new_columns
    #X_test.columns = new_columns

    # Se escribe un conjunto de datos preprocesado

    X_train.to_csv(PATH_INTERMEDIATE_DATASETS+'X_train_pp.csv', index=False)
    X_test.to_csv(PATH_INTERMEDIATE_DATASETS+'X_test_pp.csv', index=False)
    y_train.to_csv(PATH_INTERMEDIATE_DATASETS+'y_train_pp.csv', index=False)
    y_test.to_csv(PATH_INTERMEDIATE_DATASETS+'y_test_pp.csv', index=False)

    print(f'Datasets escritos en {PATH_INTERMEDIATE_DATASETS}')

@task.virtualenv(
    task_id="training_model", 
    requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn==1.5.2",
        "joblib",
        "feature-engine==1.8.2"
        ], 
    system_site_packages=False
)
def training_model():
    """Se entrena el modelo ml sobre el conjunto de datos preprocesado"""
    import sys
    PATH_BASE = '/opt/airflow/dags/'
    sys.path.append(PATH_BASE)

    import numpy as np
    import pandas as pd

    import joblib

    from sklearn.ensemble import RandomForestClassifier
    from sklearn import metrics

    # Se cargan datasets para entrenar el modelo
    PATH_INTERMEDIATE_DATASETS = PATH_BASE+'data/intermediate'+'/'
    X_train = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'X_train_pp.csv')
    y_train = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'y_train_pp.csv')

    X_test = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'X_test_pp.csv')
    y_test = pd.read_csv(PATH_INTERMEDIATE_DATASETS+'y_test_pp.csv')

    clf_rf = RandomForestClassifier()

    # Se usan los parámetros usados para entrenar el modelo R. Forest anteriormente en Pycaret
    model_params = {'bootstrap': True,
                    'ccp_alpha': 0.0,
                    'class_weight': None,
                    'criterion': 'gini',
                    'max_depth': None,
                    'max_features': 'sqrt',
                    'max_leaf_nodes': None,
                    'max_samples': None,
                    'min_impurity_decrease': 0.0,
                    'min_samples_leaf': 1,
                    'min_samples_split': 2,
                    'min_weight_fraction_leaf': 0.0,
                    'monotonic_cst': None,
                    'n_estimators': 100,
                    'n_jobs': -1,
                    'oob_score': False,
                    'random_state': 123,
                    'verbose': 0,
                    'warm_start': False}

    clf_rf.set_params(**model_params)
    clf_rf.fit(X_train.values, y_train.values)

    y_pred = clf_rf.predict(X_test.values)

    print("Accuracy:", metrics.accuracy_score(y_test, y_pred))
    print("AUC:", metrics.roc_auc_score(y_test, clf_rf.predict_proba(X_test.values), multi_class='ovr'))
    print("precision_macro:", metrics.precision_score(y_test, y_pred, average='macro'))

    joblib.dump(clf_rf, PATH_BASE +'pipelines/prange_model_pipeline.joblib')

@dag(
    dag_id='training_dag',
    default_args = default_args,
    start_date=datetime(2025, 4, 1),
    on_failure_callback = lambda context: failure_email(context),
    on_success_callback = lambda context: success_email(context),
    schedule=None
)
def training_dag():
    sp_data_task = split_training_data()
    preprocesing_tk = preprocessing_pp()
    training_model_task = training_model()

    sp_data_task >> preprocesing_tk >> training_model_task

training_dag()
