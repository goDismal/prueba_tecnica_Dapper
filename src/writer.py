import os
import logging
import pandas as pd
import psycopg2
from datetime import datetime

logger = logging.getLogger(__name__)

# Credenciales desde variables de entorno
DB_CONFIG = {
    'dbname':   os.environ.get('DB_NAME', 'airflow'),
    'user':     os.environ.get('DB_USER', 'airflow'),
    'password': os.environ.get('DB_PASSWORD', 'airflow'),
    'host':     os.environ.get('DB_HOST', 'postgres'),
    'port':     os.environ.get('DB_PORT', '5432'),
}

ENTITY_VALUE = 'Agencia Nacional de Infraestructura'


class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.connection.cursor()
            logger.info("Conexión a base de datos establecida.")
            return True
        except Exception as e:
            logger.error(f"Error conectando a la base de datos: {e}")
            return False

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Conexión a base de datos cerrada.")

    def execute_query(self, query, params=None):
        if not self.cursor:
            raise Exception("Base de datos no conectada.")
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def bulk_insert(self, df, table_name):
        if not self.connection or not self.cursor:
            raise Exception("Base de datos no conectada.")

        try:
            df = df.astype(object).where(pd.notnull(df), None)
            columns_for_sql = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))

            insert_query = f"INSERT INTO {table_name} ({columns_for_sql}) VALUES ({placeholders})"
            records_to_insert = [tuple(x) for x in df.values]

            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            return len(df)
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Error insertando en {table_name}: {str(e)}")


def insert_regulations_component(db_manager, new_ids):
    if not new_ids:
        return 0

    try:
        id_rows = pd.DataFrame(new_ids, columns=['regulations_id'])
        id_rows['components_id'] = 7

        inserted = db_manager.bulk_insert(id_rows, 'regulations_component')
        logger.info(f"Componentes insertados: {inserted}")
        return inserted
    except Exception as e:
        logger.error(f"Error insertando componentes: {e}")
        return 0


def insert_new_records(db_manager, df, entity):
    """
    Inserta nuevos registros evitando duplicados.
    Lógica idéntica al original.
    """
    regulations_table = 'regulations'

    # 1. Obtener registros existentes
    query = """
        SELECT title, created_at, entity, COALESCE(external_link, '') as external_link
        FROM {}
        WHERE entity = %s
    """.format(regulations_table)

    existing = db_manager.execute_query(query, (entity,))

    if not existing:
        db_df = pd.DataFrame(columns=['title', 'created_at', 'entity', 'external_link'])
    else:
        db_df = pd.DataFrame(existing, columns=['title', 'created_at', 'entity', 'external_link'])

    logger.info(f"Registros existentes en BD para {entity}: {len(db_df)}")

    # 2. Filtrar por entidad
    entity_df = df[df['entity'] == entity].copy()
    if entity_df.empty:
        logger.info(f"No hay registros para la entidad {entity}.")
        return 0

    logger.info(f"Registros a procesar: {len(entity_df)}")

    # 3. Normalizar para comparación
    if not db_df.empty:
        db_df['created_at'] = db_df['created_at'].astype(str)
        db_df['external_link'] = db_df['external_link'].fillna('').astype(str)
        db_df['title'] = db_df['title'].astype(str).str.strip()

    entity_df['created_at'] = entity_df['created_at'].astype(str)
    entity_df['external_link'] = entity_df['external_link'].fillna('').astype(str)
    entity_df['title'] = entity_df['title'].astype(str).str.strip()

    # 4. Identificar duplicados
    if db_df.empty:
        new_records = entity_df.copy()
        duplicates_found = 0
        logger.info("No hay registros existentes, todos son nuevos.")
    else:
        entity_df['unique_key'] = (
            entity_df['title'] + '|' +
            entity_df['created_at'] + '|' +
            entity_df['external_link']
        )
        db_df['unique_key'] = (
            db_df['title'] + '|' +
            db_df['created_at'] + '|' +
            db_df['external_link']
        )

        existing_keys = set(db_df['unique_key'])
        entity_df['is_duplicate'] = entity_df['unique_key'].isin(existing_keys)

        new_records = entity_df[~entity_df['is_duplicate']].copy()
        duplicates_found = len(entity_df) - len(new_records)

        if duplicates_found > 0:
            logger.info(f"Duplicados contra BD: {duplicates_found}")

    # 5. Remover duplicados internos
    before = len(new_records)
    new_records = new_records.drop_duplicates(
        subset=['title', 'created_at', 'external_link'],
        keep='first'
    )
    internal_duplicates = before - len(new_records)
    if internal_duplicates > 0:
        logger.info(f"Duplicados internos removidos: {internal_duplicates}")

    total_duplicates = duplicates_found + internal_duplicates
    logger.info(f"Total duplicados descartados: {total_duplicates}")
    logger.info(f"Registros finales a insertar: {len(new_records)}")

    if new_records.empty:
        logger.info("No hay registros nuevos para insertar.")
        return 0

    # 6. Limpiar columnas auxiliares
    for col in ['unique_key', 'is_duplicate']:
        if col in new_records.columns:
            new_records = new_records.drop(columns=[col])

    # 7. Insertar
    try:
        inserted = db_manager.bulk_insert(new_records, regulations_table)
        logger.info(f"Registros insertados exitosamente: {inserted}")
    except Exception as e:
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            logger.warning("Algunos registros ya existían, se omitieron.")
            return 0
        raise e

    # 8. Obtener IDs insertados
    new_ids_query = f"""
        SELECT id FROM {regulations_table}
        WHERE entity = %s
        ORDER BY id DESC
        LIMIT %s
    """
    new_ids_result = db_manager.execute_query(new_ids_query, (entity, inserted))
    new_ids = [row[0] for row in new_ids_result]

    # 9. Insertar componentes
    insert_regulations_component(db_manager, new_ids)

    return inserted


def run_writing(records: list) -> int:
    """
    Punto de entrada del módulo de escritura.
    Retorna cantidad de registros insertados.
    """
    if not records:
        logger.info("No hay registros para escribir.")
        return 0

    df = pd.DataFrame(records)
    logger.info(f"Iniciando escritura — registros a procesar: {len(df)}")

    db_manager = DatabaseManager()
    if not db_manager.connect():
        raise Exception("No se pudo conectar a la base de datos.")

    try:
        inserted = insert_new_records(db_manager, df, ENTITY_VALUE)
        logger.info(f"Escritura completa — registros insertados: {inserted}")
        return inserted
    finally:
        db_manager.close()