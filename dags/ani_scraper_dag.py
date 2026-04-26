import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

RULES_PATH = '/opt/airflow/configs/validation_rules.yaml'
NUM_PAGES = 9

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def task_extract(**context):
    from extractor import run_extraction

    logger.info("=== INICIANDO EXTRACCIÓN ===")
    records = run_extraction(num_pages=NUM_PAGES)

    if not records:
        raise ValueError("La extracción no retornó registros.")

    logger.info(f"Extracción completa — registros extraídos: {len(records)}")

    # Pasar registros a la siguiente tarea via XCom
    context['ti'].xcom_push(key='extracted_records', value=records)


def task_validate(**context):
    from validator import run_validation

    logger.info("=== INICIANDO VALIDACIÓN ===")

    records = context['ti'].xcom_pull(
        task_ids='extract',
        key='extracted_records'
    )

    if not records:
        raise ValueError("No se recibieron registros para validar.")

    validated = run_validation(records, RULES_PATH)

    logger.info(f"Validación completa — registros válidos: {len(validated)}")

    context['ti'].xcom_push(key='validated_records', value=validated)


def task_write(**context):
    from writer import run_writing

    logger.info("=== INICIANDO ESCRITURA ===")

    records = context['ti'].xcom_pull(
        task_ids='validate',
        key='validated_records'
    )

    if not records:
        logger.info("No hay registros válidos para escribir.")
        return

    inserted = run_writing(records)

    logger.info(f"Escritura completa — registros insertados: {inserted}")


with DAG(
    dag_id='ani_scraping',
    description='Extracción, validación y escritura de normativas ANI',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['ani', 'scraping'],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=task_extract,
    )

    validate = PythonOperator(
        task_id='validate',
        python_callable=task_validate,
    )

    write = PythonOperator(
        task_id='write',
        python_callable=task_write,
    )

    extract >> validate >> write