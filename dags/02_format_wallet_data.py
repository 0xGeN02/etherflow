"""DAG 2: Formateo y limpieza de datos de wallets"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import json

# Agregar utils al path
sys.path.insert(0, os.path.dirname(__file__))
from utils.wallet_utils import WalletDataManager, wei_to_eth, format_timestamp


def format_wallet_data(**context):
    """Formatea y limpia los datos raw de una wallet"""
    manager = WalletDataManager()
    
    # Obtener lista de archivos raw
    raw_files = manager.list_wallet_files(stage='raw')
    
    if not raw_files:
        print("No hay archivos raw para procesar")
        return []
    
    formatted_results = []
    
    for raw_file in raw_files:
        try:
            # Leer datos raw
            with open(raw_file, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            wallet_address = raw_data.get('wallet_address', 'unknown')
            print(f"Formateando datos de: {wallet_address}")
            
            # Formatear balance
            balance_data = raw_data.get('balance', {})
            balance_wei = balance_data.get('result', '0')
            balance_eth = wei_to_eth(balance_wei)
            
            # Formatear transacciones
            tx_data = raw_data.get('transactions', {})
            transactions_raw = tx_data.get('result', [])
            
            formatted_transactions = []
            for tx in transactions_raw:
                formatted_tx = {
                    'hash': tx.get('hash', ''),
                    'from': tx.get('from', ''),
                    'to': tx.get('to', ''),
                    'value_wei': tx.get('value', '0'),
                    'value_eth': wei_to_eth(tx.get('value', '0')),
                    'timestamp': format_timestamp(tx.get('timeStamp', '')),
                    'block_number': tx.get('blockNumber', ''),
                    'gas': tx.get('gas', '0'),
                    'gas_price': tx.get('gasPrice', '0'),
                    'gas_used': tx.get('gasUsed', '0'),
                    'is_error': tx.get('isError', '0') == '1',
                    'tx_receipt_status': tx.get('txreceipt_status', ''),
                }
                formatted_transactions.append(formatted_tx)
            
            # Crear datos formateados
            formatted_data = {
                'wallet_address': wallet_address,
                'processed_at': datetime.utcnow().isoformat(),
                'balance': {
                    'wei': balance_wei,
                    'eth': balance_eth,
                    'status': balance_data.get('status', ''),
                    'message': balance_data.get('message', ''),
                },
                'transactions': {
                    'count': len(formatted_transactions),
                    'list': formatted_transactions,
                },
                'metadata': {
                    'fetched_at': raw_data.get('fetched_at', ''),
                    'raw_file': str(raw_file),
                }
            }
            
            # Guardar datos formateados
            formatted_file = manager.save_wallet_data(wallet_address, formatted_data, stage='formatted')
            formatted_results.append({
                'wallet': wallet_address,
                'file_path': str(formatted_file),
                'status': 'success',
                'balance_eth': balance_eth,
                'tx_count': len(formatted_transactions),
            })
            
            print(f"✓ Datos formateados guardados en: {formatted_file}")
            print(f"  Balance: {balance_eth:.6f} ETH")
            print(f"  Transacciones: {len(formatted_transactions)}")
            
        except Exception as e:
            print(f"✗ Error formateando {raw_file}: {e}")
            formatted_results.append({
                'wallet': str(raw_file),
                'status': 'error',
                'error': str(e)
            })
    
    # Push resultados a XCom
    context['task_instance'].xcom_push(key='format_results', value=formatted_results)
    
    successful = [r for r in formatted_results if r['status'] == 'success']
    print(f"\n✓ Formateadas {len(successful)} de {len(formatted_results)} wallets exitosamente")
    
    return formatted_results


def validate_formatted_data(**context):
    """Valida que los datos formateados sean correctos"""
    manager = WalletDataManager()
    formatted_files = manager.list_wallet_files(stage='formatted')
    
    if not formatted_files:
        raise ValueError("No hay archivos formateados para validar")
    
    validation_results = []
    
    for file in formatted_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            wallet = data.get('wallet_address', 'unknown')
            
            # Validaciones básicas
            errors = []
            
            if 'balance' not in data:
                errors.append("Falta campo 'balance'")
            elif 'eth' not in data['balance']:
                errors.append("Falta conversión a ETH")
            
            if 'transactions' not in data:
                errors.append("Falta campo 'transactions'")
            elif 'list' not in data['transactions']:
                errors.append("Falta lista de transacciones")
            
            if errors:
                validation_results.append({
                    'wallet': wallet,
                    'status': 'invalid',
                    'errors': errors
                })
                print(f"✗ Validación fallida para {wallet}: {errors}")
            else:
                validation_results.append({
                    'wallet': wallet,
                    'status': 'valid',
                    'file': str(file)
                })
                print(f"✓ Validación exitosa para {wallet}")
                
        except Exception as e:
            validation_results.append({
                'file': str(file),
                'status': 'error',
                'error': str(e)
            })
            print(f"✗ Error validando {file}: {e}")
    
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    
    valid = [r for r in validation_results if r['status'] == 'valid']
    invalid = [r for r in validation_results if r['status'] != 'valid']
    
    print(f"\n✓ Validadas {len(valid)} wallets correctamente")
    if invalid:
        print(f"✗ {len(invalid)} wallets con errores de validación")
    
    return validation_results


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

with DAG(
    dag_id='02_format_wallet_data',
    default_args=default_args,
    description='Formatea y limpia los datos raw de wallets',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ethereum', 'transform', 'etl'],
) as dag:
    
    format_task = PythonOperator(
        task_id='format_wallet_data',
        python_callable=format_wallet_data,
    )
    
    validate_task = PythonOperator(
        task_id='validate_formatted_data',
        python_callable=validate_formatted_data,
    )
    
    format_task >> validate_task
