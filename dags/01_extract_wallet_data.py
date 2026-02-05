"""DAG 1: Extracción de datos de wallets de Ethereum desde Etherscan"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import redis
import json

# Agregar utils al path
sys.path.insert(0, os.path.dirname(__file__))
from utils.wallet_utils import WalletDataManager

# Initialize Redis client
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)


def extract_wallet(**context):
    """Extrae datos de una wallet"""

    wallet = os.environ.get('WALLET_ADDRESS')
    if wallet is None:
        raise ValueError("WALLET_ADDRESS no está configurada en las variables de entorno")
    
    manager = WalletDataManager()
    results = []
    
    try:
        print(f"Extrayendo datos de wallet: {wallet}")
        data = manager.fetch_wallet_full_data(wallet)
        
        # Guardar en Redis
        serialized_data = json.dumps(data)
        redis_key = f"wallet_data:raw:{wallet}"
        redis_client.set(redis_key, serialized_data)
        print(f"✓ Datos guardados en Redis con clave: {redis_key}")
        
        results.append({
            'wallet': wallet,
            'redis_key': redis_key,
            'status': 'success'
        })
    except Exception as e:
        print(f"✗ Error procesando wallet {wallet}: {e}")
        results.append({
            'wallet': wallet,
            'status': 'error',
            'error': str(e)
        })
    
    # Push resultados a XCom
    context['task_instance'].xcom_push(key='extraction_results', value=results)
    
    return results


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
}

with DAG(
    dag_id='01_extract_wallet_data',
    default_args=default_args,
    description='Extrae datos de wallets de Ethereum desde Etherscan API',
    schedule=None,  # Manual trigger o programado según necesidad
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ethereum', 'extract', 'etl'],
) as dag:
    
    extract_wallet_task = PythonOperator(
        task_id='extract_wallet',
        python_callable=extract_wallet,
    )
