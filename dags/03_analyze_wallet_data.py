"""DAG 3: Análisis de datos de wallets"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import json
from collections import defaultdict

# Agregar utils al path
sys.path.insert(0, os.path.dirname(__file__))
from utils.wallet_utils import WalletDataManager


def analyze_wallet_transactions(**context):
    """Analiza las transacciones de las wallets"""
    manager = WalletDataManager()
    formatted_files = manager.list_wallet_files(stage='formatted')
    
    if not formatted_files:
        print("No hay archivos formateados para analizar")
        return []
    
    analysis_results = []
    
    for file in formatted_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            wallet = data.get('wallet_address', 'unknown')
            print(f"\nAnalizando wallet: {wallet}")
            
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
                if tx_from == wallet.lower():
                    total_sent += value_eth
                    tx_by_direction['sent'] += 1
                    unique_addresses.add(tx_to)
                elif tx_to == wallet.lower():
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
                'wallet_address': wallet,
                'analyzed_at': datetime.utcnow().isoformat(),
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
                    'source_file': str(file),
                }
            }
            
            # Guardar análisis
            analyzed_file = manager.save_wallet_data(wallet, analysis, stage='analyzed')
            
            analysis_results.append({
                'wallet': wallet,
                'file_path': str(analyzed_file),
                'status': 'success',
            })
            
            print(f"✓ Análisis guardado en: {analyzed_file}")
            print(f"  Balance actual: {balance_eth:.6f} ETH")
            print(f"  Total enviado: {total_sent:.6f} ETH")
            print(f"  Total recibido: {total_received:.6f} ETH")
            print(f"  Balance neto: {net_balance:.6f} ETH")
            print(f"  Transacciones: {len(transactions)} (↑{tx_by_direction['received']} ↓{tx_by_direction['sent']})")
            print(f"  Tasa de éxito: {success_rate:.2f}%")
            print(f"  Direcciones únicas: {len(unique_addresses)}")
            
        except Exception as e:
            print(f"✗ Error analizando {file}: {e}")
            analysis_results.append({
                'file': str(file),
                'status': 'error',
                'error': str(e)
            })
    
    context['task_instance'].xcom_push(key='analysis_results', value=analysis_results)
    
    successful = [r for r in analysis_results if r['status'] == 'success']
    print(f"\n✓ Analizadas {len(successful)} de {len(analysis_results)} wallets exitosamente")
    
    return analysis_results


def generate_summary_report(**context):
    """Genera un reporte resumen de todas las wallets analizadas"""
    manager = WalletDataManager()
    analyzed_files = manager.list_wallet_files(stage='analyzed')
    
    if not analyzed_files:
        print("No hay archivos analizados para generar reporte")
        return None
    
    # Agregaciones globales
    total_wallets = len(analyzed_files)
    total_balance = 0
    total_transactions = 0
    total_sent = 0
    total_received = 0
    all_unique_addresses = set()
    
    wallet_summaries = []
    
    for file in analyzed_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                analysis = json.load(f)
            
            wallet = analysis.get('wallet_address', 'unknown')
            balance = analysis.get('current_balance', {}).get('eth', 0)
            tx_count = analysis.get('transaction_stats', {}).get('total_transactions', 0)
            sent = analysis.get('value_stats', {}).get('total_sent_eth', 0)
            received = analysis.get('value_stats', {}).get('total_received_eth', 0)
            unique_addrs = analysis.get('network_stats', {}).get('unique_addresses_interacted', 0)
            
            total_balance += balance
            total_transactions += tx_count
            total_sent += sent
            total_received += received
            
            wallet_summaries.append({
                'wallet': wallet,
                'balance': balance,
                'transactions': tx_count,
                'sent': sent,
                'received': received,
                'unique_addresses': unique_addrs,
            })
            
        except Exception as e:
            print(f"✗ Error procesando {file}: {e}")
    
    # Ordenar por balance descendente
    wallet_summaries.sort(key=lambda x: x['balance'], reverse=True)
    
    # Crear reporte resumen
    summary_report = {
        'generated_at': datetime.utcnow().isoformat(),
        'global_stats': {
            'total_wallets': total_wallets,
            'total_balance_eth': round(total_balance, 6),
            'total_transactions': total_transactions,
            'total_sent_eth': round(total_sent, 6),
            'total_received_eth': round(total_received, 6),
            'avg_balance_per_wallet': round(total_balance / total_wallets, 6) if total_wallets else 0,
            'avg_transactions_per_wallet': round(total_transactions / total_wallets, 2) if total_wallets else 0,
        },
        'wallet_summaries': wallet_summaries,
        'top_balances': wallet_summaries[:5],  # Top 5 wallets por balance
    }
    
    # Guardar reporte
    report_file = manager.data_dir / 'summary_report.json'
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(summary_report, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print("REPORTE RESUMEN DE WALLETS")
    print(f"{'='*60}")
    print(f"Total de wallets analizadas: {total_wallets}")
    print(f"Balance total: {total_balance:.6f} ETH")
    print(f"Balance promedio: {total_balance/total_wallets:.6f} ETH" if total_wallets else "N/A")
    print(f"Transacciones totales: {total_transactions}")
    print(f"Total enviado: {total_sent:.6f} ETH")
    print(f"Total recibido: {total_received:.6f} ETH")
    print(f"\nTop 5 wallets por balance:")
    for i, w in enumerate(wallet_summaries[:5], 1):
        print(f"  {i}. {w['wallet'][:10]}... - {w['balance']:.6f} ETH")
    print(f"{'='*60}")
    print(f"✓ Reporte guardado en: {report_file}\n")
    
    context['task_instance'].xcom_push(key='summary_report', value=summary_report)
    context['task_instance'].xcom_push(key='report_file', value=str(report_file))
    
    return summary_report


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
    
    analyze_task >> summary_task
