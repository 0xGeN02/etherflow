"""DAG Maestro: Pipeline completo ETL de wallets Ethereum"""
from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os


def validate_environment(**context) -> None:
    """Valida que las variables de entorno necesarias estén configuradas"""
    required_vars = ['ETHERSCAN_API_KEY']
    optional_vars = ['WALLET_ADDRESS', 'WALLET_ADDRESSES']
    
    errors = []
    warnings = []
    
    for var in required_vars:
        if not os.environ.get(var):
            errors.append(f"Falta variable requerida: {var}")
    
    # Al menos una debe estar presente
    if not os.environ.get('WALLET_ADDRESS') and not os.environ.get('WALLET_ADDRESSES'):
        warnings.append("No se especificó WALLET_ADDRESS ni WALLET_ADDRESSES")
        warnings.append("Puedes pasarlas como parámetros al ejecutar el DAG")
    
    if errors:
        error_msg = "\n".join(errors)
        raise ValueError(f"Errores de configuración:\n{error_msg}")
    
    print("✓ Validación de entorno:")
    print(f"  ETHERSCAN_API_KEY: {'configurado' if os.environ.get('ETHERSCAN_API_KEY') else 'NO CONFIGURADO'}")
    print(f"  WALLET_ADDRESS: {os.environ.get('WALLET_ADDRESS', 'no configurado')}")
    print(f"  WALLET_ADDRESSES: {os.environ.get('WALLET_ADDRESSES', 'no configurado')}")
    
    if warnings:
        print("\n⚠ Advertencias:")
        for warning in warnings:
            print(f"  - {warning}")
    
    return {'status': 'valid', 'warnings': warnings}


def log_pipeline_start(**context)-> None:
    """Registra el inicio del pipeline"""
    print("\n" + "="*70)
    print("INICIANDO PIPELINE COMPLETO DE ETL DE WALLETS ETHEREUM")
    print("="*70)
    print(f"Fecha/Hora: {datetime.utcnow().isoformat()}")
    print(f"Execution Date: {context['execution_date']}")
    print(f"Run ID: {context['run_id']}")
    print("="*70 + "\n")


def log_pipeline_end(**context) -> None:
    """Registra el fin del pipeline"""
    print("\n" + "="*70)
    print("PIPELINE COMPLETO DE ETL FINALIZADO EXITOSAMENTE")
    print("="*70)
    print(f"Fecha/Hora: {datetime.utcnow().isoformat()}")
    print(f"Execution Date: {context['execution_date']}")
    print(f"Run ID: {context['run_id']}")
    print("="*70 + "\n")


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    dag_id='00_master_etl_pipeline',
    default_args=default_args,
    description='Pipeline maestro completo: Extracción → Formateo → Análisis → Carga a BD',
    schedule='@daily',  # Ejecutar diariamente a medianoche
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ethereum', 'etl', 'master', 'pipeline'],
    params={
        'wallet_address': '',  # Wallet individual (opcional)
        'wallet_addresses': '',  # Múltiples wallets separadas por coma (opcional)
    }
) as dag:
    
    # Validación inicial
    validate_env = PythonOperator(
        task_id='validate_environment',
        python_callable=validate_environment,
    )
    
    log_start = PythonOperator(
        task_id='log_pipeline_start',
        python_callable=log_pipeline_start,
    )
    
    # Paso 1: Extracción
    extract_data = TriggerDagRunOperator(
        task_id='extract_wallet_data',
        trigger_dag_id='01_extract_wallet_data',
        wait_for_completion=True,
        poke_interval=30,
        conf={
            'wallet_address': '{{ params.wallet_address }}',
            'wallet_addresses': '{{ params.wallet_addresses }}',
        },
    )
    
    # Paso 2: Formateo
    format_data = TriggerDagRunOperator(
        task_id='format_wallet_data',
        trigger_dag_id='02_format_wallet_data',
        wait_for_completion=True,
        poke_interval=30,
    )
    
    # Paso 3: Análisis
    analyze_data = TriggerDagRunOperator(
        task_id='analyze_wallet_data',
        trigger_dag_id='03_analyze_wallet_data',
        wait_for_completion=True,
        poke_interval=30,
    )
    
    # Paso 4: Carga a BD
    load_to_db = TriggerDagRunOperator(
        task_id='load_data_to_database',
        trigger_dag_id='04_load_wallet_data_to_db',
        wait_for_completion=True,
        poke_interval=30,
    )
    
    log_end = PythonOperator(
        task_id='log_pipeline_end',
        python_callable=log_pipeline_end,
    )
    
    # Definir el flujo del pipeline
    validate_env >> log_start >> extract_data >> format_data >> analyze_data >> load_to_db >> log_end
