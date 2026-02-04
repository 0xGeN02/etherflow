# EtherFlow - Pipeline ETL de Wallets Ethereum

Pipeline completo de ETL (Extract, Transform, Load) para analizar datos de wallets de Ethereum utilizando Apache Airflow y la API de Etherscan.

## 🏗️ Arquitectura del Pipeline

El pipeline está dividido en 5 DAGs modulares:

### 1. **00_master_etl_pipeline** (DAG Maestro)
Orquesta todo el pipeline completo: Extracción → Formateo → Análisis → Carga a BD
- Valida el entorno
- Ejecuta los 4 DAGs en secuencia
- Registra logs de inicio y fin

### 2. **01_extract_wallet_data** (Extracción)
Extrae datos de wallets desde Etherscan API:
- Balance actual
- Transacciones históricas (últimas 10 por defecto)
- Guarda datos raw en `/opt/airflow/data/wallet_raw_{address}.json`

### 3. **02_format_wallet_data** (Formateo)
Limpia y formatea los datos raw:
- Convierte Wei a ETH
- Formatea timestamps
- Valida integridad de datos
- Guarda en `/opt/airflow/data/wallet_formatted_{address}.json`

### 4. **03_analyze_wallet_data** (Análisis)
Analiza los datos formateados:
- Estadísticas de transacciones (enviadas/recibidas)
- Cálculo de balance neto
- Análisis de gas usado
- Direcciones únicas con las que interactuó
- Genera reporte resumen global en `/opt/airflow/data/summary_report.json`
- Guarda análisis individual en `/opt/airflow/data/wallet_analyzed_{address}.json`

### 5. **04_load_wallet_data_to_db** (Carga)
Carga todos los datos a PostgreSQL:
- Tabla `wallet_balances`: Balance actual de cada wallet
- Tabla `wallet_transactions`: Todas las transacciones
- Tabla `wallet_analysis`: Métricas y análisis calculados

## 🚀 Inicio Rápido

### Prerequisitos
- Docker y Docker Compose
- API Key de Etherscan (gratuita en https://etherscan.io/apis)

### Configuración

1. **Configurar variables de entorno (.env)**
```bash
# Secrets
ETHERSCAN_API_KEY="tu_api_key_aqui"

# Wallets
WALLET_ADDRESS="0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE"
# O múltiples wallets:
# WALLET_ADDRESSES="0xwallet1,0xwallet2,0xwallet3"

# Airflow
AIRFLOW_UID=50000
```

2. **Iniciar servicios con Docker Compose**
```bash
# Primera vez - inicializa la base de datos
docker compose up airflow-init

# Iniciar todos los servicios
docker compose up -d
```

3. **Acceder a Airflow UI**
- URL: http://localhost:8080
- Usuario: `airflow`
- Contraseña: `airflow`

### Ejecución del Pipeline

#### Opción 1: Pipeline Completo (Recomendado)
Ejecuta el DAG maestro que orquesta todo:
```bash
docker compose run airflow-cli dags trigger 00_master_etl_pipeline
```

#### Opción 2: DAGs Individuales
```bash
# Solo extracción
docker compose run airflow-cli dags trigger 01_extract_wallet_data

# Solo formateo
docker compose run airflow-cli dags trigger 02_format_wallet_data

# Solo análisis
docker compose run airflow-cli dags trigger 03_analyze_wallet_data

# Solo carga a BD
docker compose run airflow-cli dags trigger 04_load_wallet_data_to_db
```

#### Opción 3: Con Parámetros Personalizados
```bash
# Ejecutar con una wallet específica
docker compose run airflow-cli dags trigger 00_master_etl_pipeline \
  --conf '{"wallet_address": "0xYourWalletAddress"}'

# Ejecutar con múltiples wallets
docker compose run airflow-cli dags trigger 00_master_etl_pipeline \
  --conf '{"wallet_addresses": "0xWallet1,0xWallet2,0xWallet3"}'
```

## 📁 Estructura de Archivos

```
etherflow/
├── dags/
│   ├── utils/
│   │   ├── __init__.py
│   │   └── wallet_utils.py          # Utilidades compartidas
│   ├── 00_master_etl_pipeline.py    # DAG maestro
│   ├── 01_extract_wallet_data.py    # Extracción
│   ├── 02_format_wallet_data.py     # Formateo
│   ├── 03_analyze_wallet_data.py    # Análisis
│   └── 04_load_wallet_data_to_db.py # Carga a BD
├── data/                             # Datos procesados
│   ├── wallet_raw_{address}.json
│   ├── wallet_formatted_{address}.json
│   ├── wallet_analyzed_{address}.json
│   └── summary_report.json
├── logs/                             # Logs de Airflow
├── config/                           # Configuración de Airflow
├── plugins/                          # Plugins personalizados
├── docker-compose.yaml
├── .env
└── README_PIPELINE.md
```

## 🔍 Monitoreo

### Ver estado de los DAGs
```bash
docker compose run airflow-cli dags list
```

### Ver ejecuciones de un DAG
```bash
docker compose run airflow-cli dags list-runs -d 00_master_etl_pipeline
```

### Ver logs de una tarea
```bash
docker compose run airflow-cli tasks log 00_master_etl_pipeline extract_wallet_data 2024-01-01
```

### Consultar la base de datos
```bash
# Conectar a PostgreSQL
docker compose exec postgres psql -U airflow

# Ver balances
SELECT wallet_address, balance_eth, last_updated 
FROM wallet_balances;

# Ver análisis
SELECT wallet_address, total_transactions, success_rate, net_balance_eth 
FROM wallet_analysis;

# Ver transacciones de una wallet
SELECT hash, from_address, to_address, value_eth, timestamp 
FROM wallet_transactions 
WHERE wallet_address = '0x...';
```

## 📊 Salidas del Pipeline

### Archivos JSON Generados

#### 1. wallet_raw_{address}.json
```json
{
  "wallet_address": "0x...",
  "fetched_at": "2024-01-01T00:00:00",
  "balance": {
    "status": "1",
    "message": "OK",
    "result": "1234567890000000000"
  },
  "transactions": {
    "status": "1",
    "result": [...]
  }
}
```

#### 2. wallet_formatted_{address}.json
```json
{
  "wallet_address": "0x...",
  "processed_at": "2024-01-01T00:00:00",
  "balance": {
    "wei": "1234567890000000000",
    "eth": 1.23456789
  },
  "transactions": {
    "count": 10,
    "list": [
      {
        "hash": "0x...",
        "from": "0x...",
        "to": "0x...",
        "value_eth": 0.5,
        "timestamp": "2024-01-01T00:00:00",
        ...
      }
    ]
  }
}
```

#### 3. wallet_analyzed_{address}.json
```json
{
  "wallet_address": "0x...",
  "analyzed_at": "2024-01-01T00:00:00",
  "current_balance": {
    "eth": 1.23456789
  },
  "transaction_stats": {
    "total_transactions": 10,
    "sent_count": 5,
    "received_count": 5,
    "failed_count": 0,
    "success_rate": 100.0
  },
  "value_stats": {
    "total_sent_eth": 2.5,
    "total_received_eth": 3.5,
    "net_balance_eth": 1.0,
    "avg_transaction_value_eth": 0.6
  },
  "gas_stats": {
    "total_gas_used": 250000,
    "avg_gas_per_tx": 25000.0
  },
  "network_stats": {
    "unique_addresses_interacted": 8
  }
}
```

#### 4. summary_report.json
```json
{
  "generated_at": "2024-01-01T00:00:00",
  "global_stats": {
    "total_wallets": 3,
    "total_balance_eth": 10.5,
    "total_transactions": 45,
    "avg_balance_per_wallet": 3.5
  },
  "wallet_summaries": [...],
  "top_balances": [...]
}
```

## 🛠️ Desarrollo con UV

Este proyecto usa `uv` como gestor de paquetes Python:

```bash
# Instalar dependencias
uv pip install -r requirements.txt

# Agregar una nueva dependencia
uv pip install nombre-paquete
uv pip freeze > requirements.txt

# Sincronizar desde pyproject.toml
uv pip sync
```

## 🐛 Troubleshooting

### Error: "No such file or directory: data/wallet_data.json"
- Asegúrate de que el volumen `./data:/opt/airflow/data` está montado en docker-compose.yaml
- Verifica que la carpeta `data/` existe en el directorio del proyecto

### Error: "ETHERSCAN_API_KEY not found"
- Configura la variable en el archivo `.env`
- Reinicia los contenedores: `docker compose restart`

### Error de conexión a PostgreSQL
- Verifica que el servicio postgres está corriendo: `docker compose ps`
- Comprueba la variable `DATABASE_URL` en `.env`

### Los DAGs no aparecen en la UI
- Verifica que los archivos están en la carpeta `dags/`
- Revisa los logs del dag-processor: `docker compose logs airflow-dag-processor`
- Puede tomar 30-60 segundos para que aparezcan

## 📝 Notas

- **Rate Limiting**: La API gratuita de Etherscan tiene límites de 5 llamadas/segundo
- **Datos Históricos**: Por defecto se obtienen las últimas 10 transacciones (configurable)
- **Base de Datos**: Por defecto usa PostgreSQL del docker-compose (compartida con Airflow)
- **Programación**: El DAG maestro está configurado para ejecutarse diariamente (`@daily`)

## 🔐 Seguridad

- **NUNCA** subas el archivo `.env` al control de versiones
- Mantén tu API key de Etherscan privada
- En producción, usa secretos de Airflow: `docker compose run airflow-cli connections add ...`

## 📚 Referencias

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Etherscan API](https://docs.etherscan.io/)
- [UV Package Manager](https://github.com/astral-sh/uv)

## 🤝 Contribuir

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## 📄 Licencia

Este proyecto es de código abierto y está disponible bajo la licencia MIT.
