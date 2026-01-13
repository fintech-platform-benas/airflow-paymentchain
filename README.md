# Airflow PaymentChain 💳
Sistema de orquestación ETL para procesamiento batch de transacciones de pago con **Apache Airflow**, **PySpark** y **AWS S3**.

## Características

- **Apache Airflow 2.10.4** con CeleryExecutor para ejecución distribuida
- **PySpark 3.5.0** con OpenJDK 17 para procesamiento Big Data
- **AWS S3** integración para lectura/escritura de datos
- **PostgreSQL 13** + **Redis** para metadata y cola de mensajes
- **Docker Compose** para despliegue completo

**Stack incluido:**
- `pyspark==3.5.0` `pandas==2.1.4` `boto3==1.35.0`
- `apache-airflow-providers-amazon==8.28.0`

---

## Requisitos

- **Docker** >= 20.10.0
- **Docker Compose** >= 2.0.0
- **Recursos**: 4GB RAM (8GB recomendado) | 2 CPUs | 10GB disco
- **AWS**: Credenciales con permisos S3

```bash
# Verificar instalación
docker --version && docker-compose --version
```

---

## Instalación

### 1. Clonar repositorio

```bash
git clone https://github.com/tu-usuario/airflow-paymentchain.git
cd airflow-paymentchain
```

### 2. Configurar variables de entorno

```bash
cat > .env << EOF
AIRFLOW_UID=$(id -u)
AWS_ACCESS_KEY_ID=tu_access_key_aqui
AWS_SECRET_ACCESS_KEY=tu_secret_key_aqui
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=cambiar_en_produccion
EOF
```

> **⚠️ IMPORTANTE**: Cambia las credenciales por defecto antes de producción

### 3. Desplegar

```bash
# Construir imágenes personalizadas
docker-compose build

# Inicializar base de datos
docker-compose up airflow-init

# Arrancar todos los servicios
docker-compose up -d

# Verificar estado
docker-compose ps
```

**Acceder a la UI:** http://localhost:8080
- Usuario: `airflow`
- Password: `airflow`

---

## Estructura del Proyecto

```
airflow-paymentchain/
├── dags/                    # DAGs de Airflow
│   └── payment_batch_dag.py # DAG de procesamiento de pagos
├── logs/                    # Logs de ejecución
├── plugins/                 # Plugins personalizados
├── config/                  # Configuración de Airflow
├── payment_processor/       # Scripts PySpark
│   ├── jars
│   └── process_transactions.py
├── docker-compose.yaml      # Servicios Docker
├── Dockerfile               # Imagen con PySpark
└── .env                     # Variables de entorno
```

---

## Uso

### Ejecutar DAG existente

```bash
# Desde CLI
docker exec -it airflow-paymentchain-airflow-scheduler-1 \
  airflow dags trigger payment_processor

# Desde UI: http://localhost:8080 → "payment_processor" → ▶️
```

### Crear nuevo DAG

Crear archivo en `dags/mi_dag.py`:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'tu_nombre',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
}

def mi_tarea():
    print("Ejecutando tarea")

with DAG('mi_dag', default_args=default_args, schedule_interval='@daily') as dag:
    tarea = PythonOperator(task_id='ejecutar', python_callable=mi_tarea)
```

**Validar:**
```bash
docker exec -it airflow-paymentchain-airflow-scheduler-1 \
  airflow dags list-import-errors
```

---

## Comandos Útiles

### Gestión de servicios

```bash
docker-compose up -d          # Arrancar
docker-compose down           # Detener
docker-compose restart        # Reiniciar
docker-compose logs -f        # Ver logs
docker-compose ps             # Estado
```

### Airflow CLI

```bash
# Listar DAGs
docker exec -it airflow-paymentchain-airflow-scheduler-1 airflow dags list

# Probar tarea
docker exec -it airflow-paymentchain-airflow-scheduler-1 \
  airflow tasks test payment_processor run_pyspark_processor 2024-01-01

# Gestionar usuarios
docker exec -it airflow-paymentchain-airflow-webserver-1 airflow users list
```

### Backup

```bash
# Backup de PostgreSQL
docker exec -t airflow-paymentchain-postgres-1 \
  pg_dump -U airflow airflow > backup_$(date +%Y%m%d).sql

# Restore
cat backup.sql | docker exec -i airflow-paymentchain-postgres-1 \
  psql -U airflow airflow
```

---

## Solución de Problemas

### `airflow-init` falla con exit code 1

```bash
docker-compose down -v
docker-compose build --no-cache
docker-compose up airflow-init
```

### DAG no aparece en la UI

```bash
# Ver errores de importación
docker exec -it airflow-paymentchain-airflow-scheduler-1 \
  airflow dags list-import-errors

# Ver logs
docker-compose logs airflow-scheduler | grep -i error
```

### Error de conexión AWS S3

```bash
# Verificar credenciales
docker exec -it airflow-paymentchain-airflow-worker-1 env | grep AWS

# Probar conexión
docker exec -it airflow-paymentchain-airflow-worker-1 \
  python -c "import boto3; print(boto3.client('s3').list_buckets())"
```

### Limpiar y reiniciar

```bash
docker-compose down -v        # ⚠️ Borra la base de datos
docker system prune -a
docker-compose build --no-cache
docker-compose up airflow-init
docker-compose up -d
```

---

## Arquitectura

```
Webserver (:8080) → Scheduler → Redis Queue → Celery Workers
                                                    ↓
                                            PySpark Jobs ↔ AWS S3
                                                    ↓
                                              PostgreSQL
```

**Componentes:**
- **Webserver**: UI y REST API
- **Scheduler**: Orquestador de tareas
- **Workers**: Ejecución distribuida con Celery
- **Redis**: Cola de mensajes
- **PostgreSQL**: Metadata y estado
- **PySpark**: Procesamiento de datos desde S3

---

## Producción

```bash
# Generar credenciales seguras
cat > .env << EOF
AIRFLOW_UID=$(id -u)
AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=$(openssl rand -base64 32)
AIRFLOW__CORE__LOAD_EXAMPLES=false
EOF

# Desplegar
docker-compose up -d
```

**Recomendaciones:**
- Cambiar credenciales por defecto
- Deshabilitar ejemplos (`LOAD_EXAMPLES=false`)
- Configurar backups automáticos de PostgreSQL
- Habilitar HTTPS en producción
- Usar AWS Secrets Manager para credenciales

---

## Mejoras Futuras

- [ ] Migrar a Kubernetes Executor
- [ ] Integración con Prometheus/Grafana
- [ ] Implementar Great Expectations para data quality
- [ ] CI/CD con GitHub Actions
- [ ] Secrets Backend con AWS Secrets Manager

---

## Recursos

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

---

**Versiones:** Airflow 2.10.4 | Python 3.11 | PySpark 3.5.0
