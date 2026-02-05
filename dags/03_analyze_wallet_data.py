"""DAG 3: Análisis de datos de wallets"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import sys
import json
import redis

# Agregar utils al path
sys.path.insert(0, os.path.dirname(__file__))
from utils.wallet_utils import WalletDataManager

# Initialize Redis client
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)


def analyze_wallet_transactions(**context):
    """Analiza las transacciones de las wallets"""
    # Obtener wallet address
    wallet = os.environ.get('WALLET_ADDRESS')
    if wallet is None:
        raise ValueError("WALLET_ADDRESS no está configurada en las variables de entorno")
    
    # Leer datos formateados desde Redis
    formatted_redis_key = f"wallet_data:formatted:{wallet}"
    formatted_data_str = redis_client.get(formatted_redis_key)
    
    if not formatted_data_str:
        print(f"No hay datos formateados en Redis para la wallet: {wallet}")
        return []
    
    analysis_results = []
    
    try:
        data = json.loads(formatted_data_str) # type: ignore[arg-type]
            
        wallet_address = data.get('wallet_address', 'unknown')
        print(f"\nAnalizando wallet: {wallet_address}")
        
        balance_eth = data.get('balance', {}).get('eth', 0)
        transactions = data.get('transactions', {}).get('list', [])
            
        # Análisis de transacciones
        total_sent = 0
        total_received = 0
        total_gas_used = 0
        failed_tx = 0
        unique_addresses = set()
        
        tx_by_direction = {'sent': 0, 'received': 0}
        
        for tx in transactions:
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            value_eth = tx.get('value_eth', 0)
            gas_used = int(tx.get('gas_used', 0))
            is_error = tx.get('is_error', False)
            
            # Dirección de la transacción
            if tx_from == wallet_address.lower():
                total_sent += value_eth
                tx_by_direction['sent'] += 1
                unique_addresses.add(tx_to)
            elif tx_to == wallet_address.lower():
                total_received += value_eth
                tx_by_direction['received'] += 1
                unique_addresses.add(tx_from)
            
            # Gas usado
            total_gas_used += gas_used
            
            # Transacciones fallidas
            if is_error:
                failed_tx += 1
        
        # Calcular métricas
        net_balance = total_received - total_sent
        avg_tx_value = (total_sent + total_received) / len(transactions) if transactions else 0
        success_rate = ((len(transactions) - failed_tx) / len(transactions) * 100) if transactions else 0
        
        analysis = {
            'wallet_address': wallet_address,
            'analyzed_at': datetime.now(timezone.utc).isoformat(),
            'current_balance': {
                'eth': balance_eth,
            },
            'transaction_stats': {
                'total_transactions': len(transactions),
                'sent_count': tx_by_direction['sent'],
                'received_count': tx_by_direction['received'],
                'failed_count': failed_tx,
                'success_rate': round(success_rate, 2),
            },
            'value_stats': {
                'total_sent_eth': round(total_sent, 6),
                'total_received_eth': round(total_received, 6),
                'net_balance_eth': round(net_balance, 6),
                'avg_transaction_value_eth': round(avg_tx_value, 6),
            },
            'gas_stats': {
                'total_gas_used': total_gas_used,
                'avg_gas_per_tx': round(total_gas_used / len(transactions), 2) if transactions else 0,
            },
            'network_stats': {
                'unique_addresses_interacted': len(unique_addresses),
            },
            'metadata': {
                'redis_key': formatted_redis_key,
            }
        }
        
        # Guardar análisis en Redis
        analyzed_redis_key = f"wallet_data:analyzed:{wallet_address}"
        redis_client.set(analyzed_redis_key, json.dumps(analysis))
        
        analysis_results.append({
            'wallet': wallet_address,
            'redis_key': analyzed_redis_key,
            'status': 'success',
        })
        
        print(f"✓ Análisis guardado en Redis: {analyzed_redis_key}")
        print(f"  Balance actual: {balance_eth:.6f} ETH")
        print(f"  Total enviado: {total_sent:.6f} ETH")
        print(f"  Total recibido: {total_received:.6f} ETH")
        print(f"  Balance neto: {net_balance:.6f} ETH")
        print(f"  Transacciones: {len(transactions)} (↑{tx_by_direction['received']} ↓{tx_by_direction['sent']})")
        print(f"  Tasa de éxito: {success_rate:.2f}%")
        print(f"  Direcciones únicas: {len(unique_addresses)}")
        
    except Exception as e:
        print(f"✗ Error analizando datos de {wallet}: {e}")
        analysis_results.append({
            'wallet': wallet,
            'status': 'error',
            'error': str(e)
        })
    
    context['task_instance'].xcom_push(key='analysis_results', value=analysis_results)
    
    successful = [r for r in analysis_results if r['status'] == 'success']
    print(f"\n✓ Analizadas {len(successful)} de {len(analysis_results)} wallets exitosamente")
    
    return analysis_results


def generate_summary_report(**context):
    """Genera un reporte resumen de la wallet analizada"""
    # Obtener wallet address
    wallet = os.environ.get('WALLET_ADDRESS')
    if wallet is None:
        raise ValueError("WALLET_ADDRESS no está configurada en las variables de entorno")
    
    # Leer datos analizados desde Redis
    analyzed_redis_key = f"wallet_data:analyzed:{wallet}"
    analyzed_data_str = redis_client.get(analyzed_redis_key)
    
    if not analyzed_data_str:
        print(f"No hay datos analizados en Redis para la wallet: {wallet}")
        return None
    
    try:
        analysis = json.loads(analyzed_data_str) # type: ignore[arg-type]
        
        wallet_address = analysis.get('wallet_address', 'unknown')
        balance = analysis.get('current_balance', {}).get('eth', 0)
        tx_count = analysis.get('transaction_stats', {}).get('total_transactions', 0)
        sent = analysis.get('value_stats', {}).get('total_sent_eth', 0)
        received = analysis.get('value_stats', {}).get('total_received_eth', 0)
        unique_addrs = analysis.get('network_stats', {}).get('unique_addresses_interacted', 0)
    
        # Crear reporte resumen
        summary_report = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'wallet_address': wallet_address,
            'stats': {
                'balance_eth': round(balance, 6),
                'total_transactions': tx_count,
                'total_sent_eth': round(sent, 6),
                'total_received_eth': round(received, 6),
                'unique_addresses_interacted': unique_addrs,
            }
        }
        
        # Guardar reporte en Redis
        summary_redis_key = f"wallet_data:summary:{wallet_address}"
        redis_client.set(summary_redis_key, json.dumps(summary_report))
        
        print(f"\n{'='*60}")
        print("REPORTE RESUMEN DE WALLET")
        print(f"{'='*60}")
        print(f"Wallet: {wallet_address}")
        print(f"Balance: {balance:.6f} ETH")
        print(f"Transacciones totales: {tx_count}")
        print(f"Total enviado: {sent:.6f} ETH")
        print(f"Total recibido: {received:.6f} ETH")
        print(f"Direcciones únicas: {unique_addrs}")
        print(f"{'='*60}")
        print(f"✓ Reporte guardado en Redis: {summary_redis_key}\n")
        
        context['task_instance'].xcom_push(key='summary_report', value=summary_report)
        context['task_instance'].xcom_push(key='redis_key', value=summary_redis_key)
        
        return summary_report
        
    except Exception as e:
        print(f"✗ Error generando reporte: {e}")
        return None


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

with DAG(
    dag_id='03_analyze_wallet_data',
    default_args=default_args,
    description='Analiza los datos formateados de wallets y genera métricas',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ethereum', 'analysis', 'etl'],
) as dag:
    
    analyze_task = PythonOperator(
        task_id='analyze_wallet_transactions',
        python_callable=analyze_wallet_transactions,
    )
    
    summary_task = PythonOperator(
        task_id='generate_summary_report',
        python_callable=generate_summary_report,
    )
    
    analyze_task >> summary_task # type: ignore
