# ANI Scraping — Airflow Pipeline

Scraping de normativas de la Agencia Nacional de Infraestructura (ANI),
orquestado en Airflow con tres etapas: Extracción → Validación → Escritura.

## Estructura

```
ani-airflow/
├── dags/
│   └── ani_scraping_dag.py      # DAG de Airflow
├── src/
│   ├── extractor.py             # Scraping de ANI
│   ├── validator.py             # Validación de campos
│   └── writer.py                # Escritura en Postgres
├── configs/
│   └── validation_rules.yaml   # Reglas de validación configurables
├── sql/
│   └── schema.sql               # DDL de tablas destino
├── config/
│   └── airflow.cfg              # Configuración de Airflow
├── docker-compose.yml
├── Dockerfile
├── Makefile
└── requirements.txt
```

## Requisitos

- Docker Desktop
- Git Bash (incluido con Git para Windows)

## Levantar el entorno

### Opción A — Con Make (requiere Make instalado)

```bash
make start
```

### Opción B — Manualmente en Git Bash

```bash
docker-compose down --volumes
docker-compose run --rm webserver airflow db init
docker-compose run --rm webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
docker-compose up -d
```

Esperar 30-60 segundos a que los servicios terminen de iniciar.

## Crear las tablas destino

Una vez levantado el entorno, ejecutar el DDL contra la base de datos:

```bash
docker exec -i prueba_tecnica_dapper-postgres-1 psql -U airflow -d airflow < sql/schema.sql
```

Resultado esperado:

```
CREATE TABLE
CREATE TABLE
CREATE INDEX
CREATE INDEX
```

## Acceder a Airflow

- URL: http://localhost:8080
- Usuario: `admin`
- Contraseña: `admin`

## Ejecutar el DAG

1. Ingresar a http://localhost:8080
2. Buscar el DAG `ani_scraping`
3. Activarlo con el toggle a la izquierda del nombre
4. Ejecutarlo manualmente con el botón ▶ **Trigger DAG**

El DAG corre automáticamente cada día (`@daily`).

## Etapas del pipeline

| Tarea      | Descripción                                                   |
| ---------- | ------------------------------------------------------------- |
| `extract`  | Scrapea las normativas de ani.gov.co                          |
| `validate` | Valida cada campo según las reglas del YAML                   |
| `write`    | Inserta los registros válidos en Postgres evitando duplicados |

## Validación

Las reglas de validación se configuran en `configs/validation_rules.yaml` sin tocar código:

- `required: true` — si el campo falla, se descarta la fila completa
- `required: false` — si el campo falla, queda `NULL` pero la fila se conserva

## Ver logs del pipeline

En Airflow:

1. Hacer clic en el DAG `ani_scraping`
2. Hacer clic en la ejecución más reciente
3. Hacer clic en cualquier tarea (`extract`, `validate` o `write`)
4. Hacer clic en **Logs**

Los logs muestran totales extraídos, descartados por validación y registros insertados:

```
INFO - Extracción completa — total registros extraídos: 45
INFO - Validación completa — total: 45 | válidos: 43 | descartados: 2
INFO - Escritura completa — registros insertados: 43
```

## Ver los registros insertados

Para exportar los registros a CSV y abrirlos en Excel:

```bash
docker exec -i prueba_tecnica_dapper-postgres-1 psql -U airflow -d airflow -c "\COPY regulations TO '/tmp/regulations.csv' CSV HEADER;"
docker cp prueba_tecnica_dapper-postgres-1:/tmp/regulations.csv ./regulations.csv
```

Esto genera un archivo `regulations.csv` en la raíz del proyecto.

Para ver un resumen rápido en terminal:

```bash
docker exec -i prueba_tecnica_dapper-postgres-1 psql -U airflow -d airflow -c "SELECT id, title, created_at, external_link, rtype_id FROM regulations;"
```

Para ver el total de registros insertados:

```bash
docker exec -i prueba_tecnica_dapper-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM regulations;"
```

## Variables de entorno

La conexión a Postgres usa estas variables con sus valores por defecto (coinciden con el docker-compose):

| Variable      | Default    |
| ------------- | ---------- |
| `DB_NAME`     | `airflow`  |
| `DB_USER`     | `airflow`  |
| `DB_PASSWORD` | `airflow`  |
| `DB_HOST`     | `postgres` |
| `DB_PORT`     | `5432`     |

## Idempotencia

El pipeline evita duplicados comparando cada registro contra los ya existentes en la base de datos usando la clave compuesta `title + created_at + external_link`. Los registros duplicados se omiten sin generar errores.
