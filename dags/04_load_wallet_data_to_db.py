"""DAG 4: Carga de datos a base de datos"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import sys
import json
import redis
from sqlalchemy import create_engine, Column, String, Float, Integer, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Dict as TypingDict

# Agregar utils al path
sys.path.insert(0, os.path.dirname(__file__))
from utils.wallet_utils import WalletDataManager

# Initialize Redis client
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

Base = declarative_base()


class WalletBalance(Base):
    """Modelo para balance de una wallet"""
    __tablename__ = 'wallet_balance'
    
    wallet_address = Column(String, primary_key=True)
    balance_eth = Column(Float, nullable=False)
    balance_wei = Column(String, nullable=False)
    last_updated = Column(DateTime, nullable=False)
    total_transactions = Column(Integer, default=0)
    

class WalletTransaction(Base):
    """Modelo para transacciones de una wallet"""
    __tablename__ = 'wallet_transactions'
    
    hash = Column(String, primary_key=True)
    wallet_address = Column(String, nullable=False, index=True)
    from_address = Column(String, nullable=False, index=True)
    to_address = Column(String, nullable=False, index=True)
    value_eth = Column(Float, nullable=False)
    value_wei = Column(String, nullable=False)
    timestamp = Column(String, nullable=False)
    block_number = Column(String, nullable=False)
    gas = Column(String)
    gas_price = Column(String)
    gas_used = Column(String)
    is_error = Column(Boolean, default=False)
    tx_receipt_status = Column(String)


class WalletAnalysis(Base):
    """Modelo para análisis de una wallet"""
    __tablename__ = 'wallet_analysis'
    
    wallet_address = Column(String, primary_key=True)
    analyzed_at = Column(DateTime, nullable=False)
    current_balance_eth = Column(Float)
    total_transactions = Column(Integer)
    sent_count = Column(Integer)
    received_count = Column(Integer)
    failed_count = Column(Integer)
    success_rate = Column(Float)
    total_sent_eth = Column(Float)
    total_received_eth = Column(Float)
    net_balance_eth = Column(Float)
    avg_transaction_value_eth = Column(Float)
    total_gas_used = Column(Integer)
    avg_gas_per_tx = Column(Float)
    unique_addresses_interacted = Column(Integer)
    analysis_data = Column(Text)  # JSON completo del análisis


def get_db_connection():
    """Obtiene la conexión a la base de datos"""
    # Usar PostgreSQL del docker-compose de Airflow
    db_url = os.environ.get(
        'DATABASE_URL',
        'postgresql://airflow:airflow@postgres/airflow'
    )
    
    # O usar SQLite local para desarrollo
    # db_url = 'sqlite:////opt/airflow/data/wallets.db'
    
    engine = create_engine(db_url, echo=False)
    return engine


def create_database_tables() -> TypingDict[str, str | list[str]]:
    """Crea las tablas en la base de datos si no existen"""
    try:
        engine = get_db_connection()
        Base.metadata.create_all(engine)
        print("✓ Tablas de base de datos creadas/verificadas exitosamente")
        
        # Listar tablas creadas
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f"  Tablas disponibles: {', '.join(tables)}")
        
        return {'status': 'success', 'tables': tables}
        
    except Exception as e:
        print(f"✗ Error creando tablas: {e}")
        raise


def load_wallet_balances(**context):
    """Carga los balances de wallets a la base de datos"""
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
    
    engine = get_db_connection()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    loaded_count = 0
    errors = []
    
    try:
        try:
            data = json.loads(formatted_data_str)  # type: ignore[arg-type]
                
            wallet_address = data.get('wallet_address')
            balance_data = data.get('balance', {})
            tx_data = data.get('transactions', {})
            
            # Crear o actualizar registro
            wallet_balance = session.query(WalletBalance).filter_by(
                wallet_address=wallet_address
            ).first()
                
            if wallet_balance:
                # Actualizar existente
                wallet_balance.balance_eth = balance_data.get('eth', 0)
                wallet_balance.balance_wei = balance_data.get('wei', '0')
                wallet_balance.last_updated = datetime.now(timezone.utc)  # type: ignore[assignment]
                wallet_balance.total_transactions = tx_data.get('count', 0)
            else:
                # Crear nuevo
                wallet_balance = WalletBalance(
                    wallet_address=wallet_address,
                    balance_eth=balance_data.get('eth', 0),
                    balance_wei=balance_data.get('wei', '0'),
                    last_updated=datetime.now(timezone.utc),
                    total_transactions=tx_data.get('count', 0),
                )
                session.add(wallet_balance)
            
            loaded_count += 1
            print(f"✓ Cargado balance de: {wallet_address}")
            
        except Exception as e:
            errors.append({'wallet': wallet, 'error': str(e)})
            print(f"✗ Error cargando datos de {wallet}: {e}")
        
        session.commit()
        print(f"\n✓ Cargados {loaded_count} balances de wallets a la base de datos")
        
    except Exception as e:
        session.rollback()
        print(f"✗ Error en la transacción: {e}")
        raise
    finally:
        session.close()
    
    context['task_instance'].xcom_push(key='loaded_balances', value=loaded_count)
    return {'loaded': loaded_count, 'errors': errors}


def load_wallet_transactions(**context):
    """Carga las transacciones de wallets a la base de datos"""
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
    
    engine = get_db_connection()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    loaded_count = 0
    skipped_count = 0
    errors = []
    
    try:
        try:
            data = json.loads(formatted_data_str)  # type: ignore[arg-type]
                
            wallet_address = data.get('wallet_address')
            transactions = data.get('transactions', {}).get('list', [])
            
            for tx in transactions:
                tx_hash = tx.get('hash')
                
                # Verificar si ya existe
                existing_tx = session.query(WalletTransaction).filter_by(
                    hash=tx_hash
                ).first()
                
                if existing_tx:
                    skipped_count += 1
                    continue
                
                # Crear nueva transacción
                wallet_tx = WalletTransaction(
                    hash=tx_hash,
                    wallet_address=wallet_address,
                    from_address=tx.get('from', ''),
                    to_address=tx.get('to', ''),
                    value_eth=tx.get('value_eth', 0),
                    value_wei=tx.get('value_wei', '0'),
                    timestamp=tx.get('timestamp', ''),
                    block_number=tx.get('block_number', ''),
                    gas=tx.get('gas', '0'),
                    gas_price=tx.get('gas_price', '0'),
                    gas_used=tx.get('gas_used', '0'),
                    is_error=tx.get('is_error', False),
                    tx_receipt_status=tx.get('tx_receipt_status', ''),
                )
                session.add(wallet_tx)
                loaded_count += 1
            
            print(f"✓ Cargadas transacciones de: {wallet_address} ({len(transactions)} tx)")
            
        except Exception as e:
            errors.append({'wallet': wallet, 'error': str(e)})
            print(f"✗ Error cargando transacciones de {wallet}: {e}")
        
        session.commit()
        print(f"\n✓ Cargadas {loaded_count} transacciones nuevas a la base de datos")
        if skipped_count > 0:
            print(f"  (Saltadas {skipped_count} transacciones duplicadas)")
        
    except Exception as e:
        session.rollback()
        print(f"✗ Error en la transacción: {e}")
        raise
    finally:
        session.close()
    
    context['task_instance'].xcom_push(key='loaded_transactions', value=loaded_count)
    return {'loaded': loaded_count, 'skipped': skipped_count, 'errors': errors}


def load_wallet_analysis(**context):
    """Carga los análisis de wallets a la base de datos"""
    # Obtener wallet address
    wallet = os.environ.get('WALLET_ADDRESS')
    if wallet is None:
        raise ValueError("WALLET_ADDRESS no está configurada en las variables de entorno")
    
    # Leer datos analizados desde Redis
    analyzed_redis_key = f"wallet_data:analyzed:{wallet}"
    analyzed_data_str = redis_client.get(analyzed_redis_key)
    
    if not analyzed_data_str:
        print(f"No hay datos analizados en Redis para la wallet: {wallet}")
        return []
    
    engine = get_db_connection()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    loaded_count = 0
    errors = []
    
    try:
        try:
            analysis = json.loads(analyzed_data_str)  # type: ignore[arg-type]
            
            wallet_address = analysis.get('wallet_address')
            tx_stats = analysis.get('transaction_stats', {})
            value_stats = analysis.get('value_stats', {})
            gas_stats = analysis.get('gas_stats', {})
            network_stats = analysis.get('network_stats', {})
            
            # Crear o actualizar registro
            wallet_analysis = session.query(WalletAnalysis).filter_by(
                wallet_address=wallet_address
            ).first()
            
            if wallet_analysis:
                # Actualizar existente
                wallet_analysis.analyzed_at = datetime.fromisoformat(  # type: ignore[assignment]
                    analysis.get('analyzed_at', datetime.utcnow().isoformat())
                )
            else:
                # Crear nuevo
                wallet_analysis = WalletAnalysis(
                    wallet_address=wallet_address,
                    analyzed_at=datetime.fromisoformat(
                        analysis.get('analyzed_at', datetime.utcnow().isoformat())
                    ),
                )
                session.add(wallet_analysis)
            
            # Actualizar campos
            wallet_analysis.current_balance_eth = analysis.get('current_balance', {}).get('eth')
            wallet_analysis.total_transactions = tx_stats.get('total_transactions')
            wallet_analysis.sent_count = tx_stats.get('sent_count')
            wallet_analysis.received_count = tx_stats.get('received_count')
            wallet_analysis.failed_count = tx_stats.get('failed_count')
            wallet_analysis.success_rate = tx_stats.get('success_rate')
            wallet_analysis.total_sent_eth = value_stats.get('total_sent_eth')
            wallet_analysis.total_received_eth = value_stats.get('total_received_eth')
            wallet_analysis.net_balance_eth = value_stats.get('net_balance_eth')
            wallet_analysis.avg_transaction_value_eth = value_stats.get('avg_transaction_value_eth')
            wallet_analysis.total_gas_used = gas_stats.get('total_gas_used')
            wallet_analysis.avg_gas_per_tx = gas_stats.get('avg_gas_per_tx')
            wallet_analysis.unique_addresses_interacted = network_stats.get('unique_addresses_interacted')
            wallet_analysis.analysis_data = json.dumps(analysis)  # type: ignore[assignment]
            
            loaded_count += 1
            print(f"✓ Cargado análisis de: {wallet_address}")
            
        except Exception as e:
            errors.append({'wallet': wallet, 'error': str(e)})
            print(f"✗ Error cargando análisis de {wallet}: {e}")
        
        session.commit()
        print(f"\n✓ Cargados {loaded_count} análisis de wallets a la base de datos")
        
    except Exception as e:
        session.rollback()
        print(f"✗ Error en la transacción: {e}")
        raise
    finally:
        session.close()
    
    context['task_instance'].xcom_push(key='loaded_analysis', value=loaded_count)
    return {'loaded': loaded_count, 'errors': errors}


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=15),
}

with DAG(
    dag_id='04_load_wallet_data_to_db',
    default_args=default_args,
    description='Carga los datos de wallets a la base de datos PostgreSQL',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ethereum', 'load', 'etl', 'database'],
) as dag:
    
    create_tables_task = PythonOperator(
        task_id='create_database_tables',
        python_callable=create_database_tables,
    )
    
    load_balances_task = PythonOperator(
        task_id='load_wallet_balances',
        python_callable=load_wallet_balances,
    )
    
    load_transactions_task = PythonOperator(
        task_id='load_wallet_transactions',
        python_callable=load_wallet_transactions,
    )
    
    load_analysis_task = PythonOperator(
        task_id='load_wallet_analysis',
        python_callable=load_wallet_analysis,
    )
    
    # Secuencia de ejecución
    create_tables_task >> [load_balances_task, load_transactions_task, load_analysis_task] # type: ignore
