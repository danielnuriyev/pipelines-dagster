import os
from pathlib import Path

import trino
import yaml
from dagster import (
    Config,
    DagsterRunStatus,
    Definitions,
    OpExecutionContext,
    RunRequest,
    job,
    op,
    run_status_sensor,
)

# Load config from YAML file
PIPELINES_DIR = Path(os.environ.get("PIPELINES_CONFIG_DIR", "/app/pipelines"))


def load_pipeline_config(name: str) -> dict:
    """Load pipeline configuration from YAML file."""
    config_path = PIPELINES_DIR / f"{name}.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


# Load configs for both pipelines
_test_a_config = load_pipeline_config("test_a_trino_insert_select")
_test_b_config = load_pipeline_config("test_b_trino_insert_select")


class TrinoInsertSelectConfig(Config):
    host: str = "localhost"
    port: int = 8080
    user: str = "dagster"
    select_query: str = ""
    target_catalog: str = ""
    target_schema: str = ""
    target_table: str = ""


def make_config_class(config_dict: dict):
    """Create a Config class with defaults from a dictionary."""

    class ConfigWithDefaults(Config):
        host: str = config_dict.get("host", "localhost")
        port: int = config_dict.get("port", 8080)
        user: str = config_dict.get("user", "dagster")
        select_query: str = config_dict.get("select_query", "")
        target_catalog: str = config_dict.get("target_catalog", "")
        target_schema: str = config_dict.get("target_schema", "")
        target_table: str = config_dict.get("target_table", "")

    return ConfigWithDefaults


TestAConfig = make_config_class(_test_a_config)
TestBConfig = make_config_class(_test_b_config)


def execute_trino_insert_select(context: OpExecutionContext, config: TrinoInsertSelectConfig):
    """Execute INSERT INTO target_table SELECT ... with configurable source and target."""
    conn = trino.dbapi.connect(
        host=config.host,
        port=config.port,
        user=config.user,
    )
    cursor = conn.cursor()

    target_full_name = f"{config.target_catalog}.{config.target_schema}.{config.target_table}"

    # Check if target table exists
    cursor.execute(f"""
        SELECT table_name FROM {config.target_catalog}.information_schema.tables
        WHERE table_catalog = '{config.target_catalog}'
        AND table_schema = '{config.target_schema}'
        AND table_name = '{config.target_table}'
    """)
    table_exists = len(cursor.fetchall()) > 0

    if table_exists:
        context.log.info(f"Table {target_full_name} exists. Inserting data...")
        cursor.execute(f"INSERT INTO {target_full_name} {config.select_query}")
    else:
        context.log.info(f"Table {target_full_name} does not exist. Creating...")
        cursor.execute(f"CREATE TABLE {target_full_name} AS {config.select_query}")

    cursor.fetchall()  # Wait for query to complete
    context.log.info("Insert complete")

    cursor.close()
    conn.close()


@op
def noop_op():
    """An op that does nothing."""
    pass


@job
def noop_job():
    """A job that does nothing."""
    noop_op()


@op
def test_a_trino_insert_select_op(context: OpExecutionContext, config: TestAConfig):
    """Copy test_a to test_b."""
    execute_trino_insert_select(context, config)


@op
def test_b_trino_insert_select_op(context: OpExecutionContext, config: TestBConfig):
    """Copy test_b to test_c."""
    execute_trino_insert_select(context, config)


@job
def test_a_trino_insert_select():
    """Copy data from test_a to test_b in Trino."""
    test_a_trino_insert_select_op()


@job
def test_b_trino_insert_select():
    """Copy data from test_b to test_c in Trino."""
    test_b_trino_insert_select_op()


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[test_a_trino_insert_select],
    request_job=test_b_trino_insert_select,
)
def test_a_success_sensor(context):
    """Trigger test_b job when test_a completes successfully."""
    return RunRequest()


defs = Definitions(
    jobs=[noop_job, test_a_trino_insert_select, test_b_trino_insert_select],
    sensors=[test_a_success_sensor],
)
