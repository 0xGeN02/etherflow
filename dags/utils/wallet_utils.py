"""Utilidades para manejo de datos de wallets de Ethereum"""
import json
import os
from typing import Dict, List, Any
from pathlib import Path
import requests
from datetime import datetime


class WalletDataManager:
    """Gestor de datos de wallets de Ethereum"""
    
    def __init__(self, data_dir: str = '/opt/airflow/data'):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.api_key = os.environ.get('ETHERSCAN_API_KEY')
        
    def get_wallet_short_address(self, wallet_address: str) -> str:
        """Obtiene los primeros 6 caracteres de la dirección de la wallet"""
        if wallet_address.startswith('0x'):
            return wallet_address[2:8].lower()
        return wallet_address[:6].lower()
    
    def get_wallet_file_path(self, wallet_address: str, stage: str = 'raw') -> Path:
        """Genera la ruta del archivo para una wallet en una etapa específica
        
        Args:
            wallet_address: Dirección de la wallet
            stage: Etapa del pipeline ('raw', 'formatted', 'analyzed')
        """
        short_addr = self.get_wallet_short_address(wallet_address)
        return self.data_dir / f"wallet_{stage}_{short_addr}.json"
    
    def fetch_wallet_balance(self, wallet_address: str) -> Dict[str, Any]:
        """Obtiene el balance de una wallet desde Etherscan"""
        url = f'https://api.etherscan.io/v2/api'
        params = {
            'chainid': '1',
            'module': 'account',
            'action': 'balance',
            'address': wallet_address,
            'tag': 'latest',
            'apikey': self.api_key
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        data['fetched_at'] = datetime.utcnow().isoformat()
        data['wallet_address'] = wallet_address
        return data
    
    def fetch_wallet_transactions(self, wallet_address: str, limit: int = 10) -> Dict[str, Any]:
        """Obtiene las transacciones de una wallet desde Etherscan"""
        url = f'https://api.etherscan.io/v2/api'
        params = {
            'chainid': '1',
            'module': 'account',
            'action': 'txlist',
            'address': wallet_address,
            'startblock': 0,
            'endblock': 99999999,
            'page': 1,
            'offset': limit,
            'sort': 'desc',
            'apikey': self.api_key
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        data['fetched_at'] = datetime.utcnow().isoformat()
        data['wallet_address'] = wallet_address
        return data
    
    def fetch_wallet_full_data(self, wallet_address: str) -> Dict[str, Any]:
        """Obtiene datos completos de una wallet (balance + transacciones)"""
        balance_data = self.fetch_wallet_balance(wallet_address)
        tx_data = self.fetch_wallet_transactions(wallet_address)
        
        full_data = {
            'wallet_address': wallet_address,
            'fetched_at': datetime.utcnow().isoformat(),
            'balance': balance_data,
            'transactions': tx_data
        }
        return full_data
    
    def save_wallet_data(self, wallet_address: str, data: Dict[str, Any], stage: str = 'raw') -> Path:
        """Guarda los datos de una wallet en un archivo JSON"""
        file_path = self.get_wallet_file_path(wallet_address, stage)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return file_path
    
    def load_wallet_data(self, wallet_address: str, stage: str = 'raw') -> Dict[str, Any]:
        """Carga los datos de una wallet desde un archivo JSON"""
        file_path = self.get_wallet_file_path(wallet_address, stage)
        if not file_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def list_wallet_files(self, stage: str = 'raw') -> List[Path]:
        """Lista todos los archivos de una wallet en una etapa específica"""
        pattern = f"wallet_{stage}_*.json"
        return list(self.data_dir.glob(pattern))


def wei_to_eth(wei_value: str) -> float:
    """Convierte Wei a ETH"""
    try:
        return int(wei_value) / 1e18
    except (ValueError, TypeError):
        return 0.0


def format_timestamp(timestamp: str) -> str:
    """Formatea un timestamp Unix a formato ISO"""
    try:
        return datetime.fromtimestamp(int(timestamp)).isoformat()
    except (ValueError, TypeError):
        return timestamp
