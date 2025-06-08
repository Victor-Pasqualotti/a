import sys
import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime
from typing import Optional

# Configurações fixas
ATHENA_DATABASE = "meu_database"
TABELA_CONTROLE = "tabela_controle_monitoramento"
GLUE_JOB_NAME = "meu_glue_etl"
REGIAO = "us-east-1"

# Inicializa clientes
glue = boto3.client("glue", region_name=REGIAO)
s3 = boto3.client("s3")

def get_latest_partition(table_name: str, database: str) -> Optional[str]:
    try:
        partitions = wr.catalog.get_partitions(database=database, table=table_name)
        if partitions.empty:
            return None
        partition_cols = wr.catalog.get_table_parameters(database, table_name)["PartitionKeys"]
        last_partition_col = partition_cols[-1]["Name"]
        latest_value = partitions.sort_values(by=last_partition_col, ascending=False).iloc[0][last_partition_col]
        return str(latest_value)
    except Exception as e:
        print(f"[ERRO] get_latest_partition({table_name}): {e}")
        return None

def get_last_recorded_partition(table_name: str) -> Optional[str]:
    query = f"""
        SELECT max(particao) AS ultima_particao
        FROM {TABELA_CONTROLE}
        WHERE tabela = '{table_name}'
    """
    try:
        df = wr.athena.read_sql_query(query, database=ATHENA_DATABASE)
        return str(df["ultima_particao"].iloc[0]) if not df.empty else None
    except Exception as e:
        print(f"[ERRO] get_last_recorded_partition({table_name}): {e}")
        return None

def estimate_partition_size_mb(table_name: str, partition_value: str) -> int:
    try:
        table_location = wr.catalog.get_table_location(database=ATHENA_DATABASE, table=table_name)
        bucket, *prefix_path = table_location.replace("s3://", "").split("/", 1)
        partition_prefix = f"{prefix_path[0]}/particao={partition_value}/"
        
        total_size = 0
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=partition_prefix):
            for obj in page.get("Contents", []):
                total_size += obj["Size"]

        return total_size // (1024 * 1024)
    except Exception as e:
        print(f"[ERRO] estimate_partition_size_mb({table_name}): {e}")
        return 0

def choose_optimal_dpu(tamanho_mb: int) -> float:
    if tamanho_mb < 512:
        return 2.0
    elif tamanho_mb < 2048:
        return 5.0
    elif tamanho_mb < 8192:
        return 10.0
    else:
        return 15.0

def dispatch_job_if_updated(table_name: str, latest_partition: str):
    previous_partition = get_last_recorded_partition(table_name)
    
    if previous_partition != latest_partition:
        print(f"[INFO] Nova partição em {table_name}: {latest_partition} (antes: {previous_partition})")
        tamanho_mb = estimate_partition_size_mb(table_name, latest_partition)
        dpu = choose_optimal_dpu(tamanho_mb)

        print(f"[INFO] Disparando job com {dpu} DPUs...")
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--tabela": table_name,
                "--particao": latest_partition
            },
            MaxCapacity=dpu
        )
        print(f"[INFO] JobRunId: {response['JobRunId']}")
    else:
        print(f"[OK] {table_name} sem novas partições.")

def main():
    try:
        tables = wr.catalog.get_tables(database=ATHENA_DATABASE)["Name"]
        print(f"[INFO] Tabelas encontradas: {len(tables)}")
        for table in tables:
            latest = get_latest_partition(table, ATHENA_DATABASE)
            if latest:
                dispatch_job_if_updated(table, latest)
    except Exception as e:
        print(f"[FATAL] Erro geral no monitoramento: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
