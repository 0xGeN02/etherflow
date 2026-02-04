"""DAG 1: Extracción de datos de wallets de Ethereum desde Etherscan"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Agregar utils al path
sys.path.insert(0, os.path.dirname(__file__))
from utils.wallet_utils import WalletDataManager


def extract_single_wallet(**context):
    """Extrae datos de una wallet individual"""
    wallet_address = context['params'].get('wallet_address') or os.environ.get('WALLET_ADDRESS')
    
    if not wallet_address:
        raise ValueError("No se especificó WALLET_ADDRESS")
    
    manager = WalletDataManager()
    print(f"Extrayendo datos de wallet: {wallet_address}")
    
    # Obtener datos completos de la wallet
    data = manager.fetch_wallet_full_data(wallet_address)
    
    # Guardar datos raw
    file_path = manager.save_wallet_data(wallet_address, data, stage='raw')
    print(f"Datos guardados en: {file_path}")
    
    # Push a XCom para siguiente tarea
    context['task_instance'].xcom_push(key='wallet_address', value=wallet_address)
    context['task_instance'].xcom_push(key='file_path', value=str(file_path))
    
    return str(file_path)


def extract_multiple_wallets(**context):
    """Extrae datos de múltiples wallets"""
    # Obtener lista de wallets desde variable o environment
    wallets_str = context['params'].get('wallet_addresses') or os.environ.get('WALLET_ADDRESSES', '')
    
    if not wallets_str:
        # Si no hay múltiples, usar la wallet por defecto
        wallets = [os.environ.get('WALLET_ADDRESS')]
    else:
        # Separar por comas
        wallets = [w.strip() for w in wallets_str.split(',') if w.strip()]
    
    manager = WalletDataManager()
    results = []
    
    for wallet in wallets:
        try:
            print(f"Extrayendo datos de wallet: {wallet}")
            data = manager.fetch_wallet_full_data(wallet)
            file_path = manager.save_wallet_data(wallet, data, stage='raw')
            results.append({
                'wallet': wallet,
                'file_path': str(file_path),
                'status': 'success'
            })
            print(f"✓ Datos guardados en: {file_path}")
        except Exception as e:
            print(f"✗ Error procesando wallet {wallet}: {e}")
            results.append({
                'wallet': wallet,
                'status': 'error',
                'error': str(e)
            })
    
    # Push resultados a XCom
    context['task_instance'].xcom_push(key='extraction_results', value=results)
    
    successful = [r for r in results if r['status'] == 'success']
    print(f"\n✓ Extraídas {len(successful)} de {len(results)} wallets exitosamente")
    
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
    params={
        'wallet_address': '',  # Wallet individual (opcional)
        'wallet_addresses': '',  # Múltiples wallets separadas por coma (opcional)
    }
) as dag:
    
    extract_multiple = PythonOperator(
        task_id='extract_multiple_wallets',
        python_callable=extract_multiple_wallets,
    )
