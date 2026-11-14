"""Automated DAG Integrity and Validation Suite for Apache Airflow.

Validates the Medallion Architecture Orchestration DAG (`tech_market_etl_pipeline`):
1. DagBag loading with 0 syntax or import errors.
2. Acyclic graph validation (no cycles / infinite loops).
3. DAG parameters: schedule ("0 18 * * 1-5"), catchup=False, max_active_runs=1,
   and retry policy (retries=2, retry_delay=5 minutes).
4. Exact task definitions (3 tasks):
   `extract_bronze_task`, `transform_silver_task`, `load_gold_task`.
5. Strict sequential Medallion architecture dependencies:
   `extract_bronze_task >> transform_silver_task >> load_gold_task`.
6. AST-level static validation for non-Airflow execution environments.
"""

import ast
from datetime import timedelta
import os
from pathlib import Path
import sys
from typing import Any, Dict, Set
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root and dags directory are available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = PROJECT_ROOT / "dags"
SRC_DIR = PROJECT_ROOT / "src"

for path in (PROJECT_ROOT, DAGS_DIR, SRC_DIR):
    str_path = str(path)
    if str_path not in sys.path:
        sys.path.insert(0, str_path)

# Verify if apache-airflow is installed in the current environment
try:
    import airflow
    from airflow.models import DAG, DagBag
    from airflow.utils.dag_cycle_tester import check_cycle

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


# ==============================================================================
# Suite 1: AST & Static Code Integrity (Always runs in any environment)
# ==============================================================================
class TestDAGStaticStructure:
    """Static AST validation of dags/finance_etl_dag.py.

    Guarantees that the DAG file exists, compiles with zero syntax errors,
    and defines the expected configuration parameters even when Airflow is not
    installed in the local host environment.
    """

    @property
    def dag_path(self) -> Path:
        """Return path to dags/finance_etl_dag.py."""
        return DAGS_DIR / "finance_etl_dag.py"

    def test_dag_file_exists(self) -> None:
        """Verify that dags/finance_etl_dag.py exists in the repository."""
        assert self.dag_path.is_file(), f"DAG file does not exist at {self.dag_path}"

    def test_dag_syntax_compilation(self) -> None:
        """Verify that dags/finance_etl_dag.py compiles without syntax errors."""
        with open(self.dag_path, "r", encoding="utf-8") as f:
            code = f.read()

        compiled = compile(code, str(self.dag_path), "exec")
        assert compiled is not None, "Failed to compile dags/finance_etl_dag.py"

    def test_ast_contains_required_definitions(self) -> None:
        """Verify via AST parsing that required constants, DAG ID and tasks are declared."""
        with open(self.dag_path, "r", encoding="utf-8") as f:
            code = f.read()

        parsed_ast = ast.parse(code)
        code_str = ast.unparse(parsed_ast)

        # Check DAG identifier
        assert "tech_market_etl_pipeline" in code_str, "DAG id 'tech_market_etl_pipeline' missing in code"

        # Check schedule
        assert "0 18 * * 1-5" in code_str, "Cron schedule '0 18 * * 1-5' missing in code"

        # Check task identifiers
        assert "extract_bronze_task" in code_str, "Task 'extract_bronze_task' missing in code"
        assert "transform_silver_task" in code_str, "Task 'transform_silver_task' missing in code"
        assert "load_gold_task" in code_str, "Task 'load_gold_task' missing in code"

        # Check sequential chaining
        assert (
            "extract_bronze_task >> transform_silver_task >> load_gold_task" in code_str
            or (
                "extract_bronze_task >> transform_silver_task" in code_str
                and "transform_silver_task >> load_gold_task" in code_str
            )
        ), "Sequential dependency chaining (extract >> transform >> load) missing"


# ==============================================================================
# Suite 2: Airflow DagBag & Dependency Integrity (Runs in Docker and CI)
# ==============================================================================
@pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="apache-airflow is not installed in local environment (runs in Docker and CI pipeline)",
)
class TestAirflowDAGIntegrity:
    """Dynamic DAG validation using Apache Airflow's DagBag parser.

    Validates:
    - 0 import errors or syntax exceptions during parsing.
    - Presence of DAG 'tech_market_etl_pipeline'.
    - Acyclic nature (no deadlocks or loops).
    - Schedule, retry policies, and catchup settings.
    - Exactly 3 tasks with proper sequential Medallion dependencies.
    """

    EXPECTED_DAG_ID = "tech_market_etl_pipeline"
    EXPECTED_TASKS = {
        "extract_bronze_task",
        "transform_silver_task",
        "load_gold_task",
    }

    @pytest.fixture(scope="class")
    def dagbag(self) -> "DagBag":
        """Instantiate DagBag to parse all DAGs in ./dags."""
        bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
        return bag

    def test_dagbag_no_import_errors(self, dagbag: "DagBag") -> None:
        """Verify that DagBag has zero import errors."""
        import_errors = dagbag.import_errors
        error_count = len(import_errors)
        if error_count > 0:
            formatted_errors = "\n".join(
                f"- File: {filepath}\n  Error: {err}"
                for filepath, err in import_errors.items()
            )
            pytest.fail(f"DagBag encountered {error_count} import error(s):\n{formatted_errors}")

    def test_dag_loaded_successfully(self, dagbag: "DagBag") -> None:
        """Verify that 'tech_market_etl_pipeline' exists and is loaded."""
        assert self.EXPECTED_DAG_ID in dagbag.dags, (
            f"DAG '{self.EXPECTED_DAG_ID}' was not found in DagBag. "
            f"Available DAGs: {list(dagbag.dags.keys())}"
        )
        dag = dagbag.get_dag(self.EXPECTED_DAG_ID)
        assert dag is not None, f"DAG '{self.EXPECTED_DAG_ID}' resolved to None."

    def test_dag_no_cycles(self, dagbag: "DagBag") -> None:
        """Verify that the DAG is strictly acyclic (no circular dependencies)."""
        dag = dagbag.get_dag(self.EXPECTED_DAG_ID)
        # Airflow's built-in cycle check raises AirflowDagCycleException if a loop exists
        check_cycle(dag)

    def test_dag_configuration_and_retries(self, dagbag: "DagBag") -> None:
        """Verify schedule, catchup, max active runs, and retry policies."""
        dag = dagbag.get_dag(self.EXPECTED_DAG_ID)

        # Schedule: Mon-Fri post-market close at 18:00 UTC
        schedule = getattr(dag, "schedule", None) or dag.schedule_interval
        assert str(schedule) == "0 18 * * 1-5", (
            f"Expected schedule '0 18 * * 1-5', got '{schedule}'"
        )

        # Execution limits
        assert dag.catchup is False, "catchup must be False to prevent historic backfills"
        assert dag.max_active_runs == 1, "max_active_runs must be 1 to prevent overlapping runs"

        # Default args / Retry policy
        default_args = dag.default_args or {}
        assert default_args.get("owner") == "data_engineering", "DAG owner must be 'data_engineering'"
        assert default_args.get("retries") == 2, "DAG retries must be 2"
        assert default_args.get("retry_delay") == timedelta(minutes=5), (
            f"DAG retry_delay must be 5 minutes, got {default_args.get('retry_delay')}"
        )
        assert default_args.get("depends_on_past") is False, "depends_on_past should be False"

    def test_dag_has_exact_expected_tasks(self, dagbag: "DagBag") -> None:
        """Verify that the DAG contains exactly the 3 Medallion tasks."""
        dag = dagbag.get_dag(self.EXPECTED_DAG_ID)
        actual_tasks = set(dag.task_dict.keys())

        assert len(dag.tasks) == 3, f"Expected 3 tasks in DAG, but found {len(dag.tasks)}"
        assert actual_tasks == self.EXPECTED_TASKS, (
            f"Task mismatch! Expected: {self.EXPECTED_TASKS}, Found: {actual_tasks}"
        )

    def test_dag_sequential_dependencies(self, dagbag: "DagBag") -> None:
        """Verify sequential dependency: extract_bronze_task >> transform_silver_task >> load_gold_task."""
        dag = dagbag.get_dag(self.EXPECTED_DAG_ID)

        extract_task = dag.get_task("extract_bronze_task")
        transform_task = dag.get_task("transform_silver_task")
        load_task = dag.get_task("load_gold_task")

        # 1. extract_bronze_task
        assert extract_task.upstream_task_ids == set(), (
            f"extract_bronze_task must have no upstream tasks, found {extract_task.upstream_task_ids}"
        )
        assert extract_task.downstream_task_ids == {"transform_silver_task"}, (
            f"extract_bronze_task downstream must be {{'transform_silver_task'}}, "
            f"found {extract_task.downstream_task_ids}"
        )

        # 2. transform_silver_task
        assert transform_task.upstream_task_ids == {"extract_bronze_task"}, (
            f"transform_silver_task upstream must be {{'extract_bronze_task'}}, "
            f"found {transform_task.upstream_task_ids}"
        )
        assert transform_task.downstream_task_ids == {"load_gold_task"}, (
            f"transform_silver_task downstream must be {{'load_gold_task'}}, "
            f"found {transform_task.downstream_task_ids}"
        )

        # 3. load_gold_task
        assert load_task.upstream_task_ids == {"transform_silver_task"}, (
            f"load_gold_task upstream must be {{'transform_silver_task'}}, "
            f"found {load_task.upstream_task_ids}"
        )
        assert load_task.downstream_task_ids == set(), (
            f"load_gold_task must have no downstream tasks, found {load_task.downstream_task_ids}"
        )


# ==============================================================================
# Suite 3: Task Execution & Robust Exception Handling
# ==============================================================================
@pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="apache-airflow is required to test task callable execution",
)
class TestTaskExecutionLogic:
    """Validates that DAG task callables raise RuntimeError on pipeline failures to trigger retries."""

    def test_extract_bronze_raises_on_failure(self) -> None:
        """Verify execute_extract_bronze raises RuntimeError when report status is not SUCCESS."""
        from dags.finance_etl_dag import execute_extract_bronze

        with patch("src.extract_s3.TechMarketExtractionPipeline") as mock_pipeline_cls:
            mock_instance = MagicMock()
            mock_instance.run.return_value = {
                "status": "FAILURE",
                "failed_tickers": [{"ticker": "NVDA", "error": "Timeout"}],
            }
            mock_pipeline_cls.return_value = mock_instance

            with pytest.raises(RuntimeError, match="Bronze layer extraction completed with issues"):
                execute_extract_bronze()

    def test_transform_silver_raises_on_failure(self) -> None:
        """Verify execute_transform_silver raises RuntimeError when report status is not SUCCESS."""
        from dags.finance_etl_dag import execute_transform_silver

        with patch("src.transform.MarketDataTransformer") as mock_transformer_cls:
            mock_instance = MagicMock()
            mock_instance.run.return_value = {
                "status": "PARTIAL_FAILURE",
                "failed_tickers": [{"ticker": "TSM", "error": "Corrupt data"}],
            }
            mock_transformer_cls.return_value = mock_instance

            with pytest.raises(RuntimeError, match="Silver layer transformation completed with issues"):
                execute_transform_silver()

    def test_load_gold_raises_on_failure(self) -> None:
        """Verify execute_load_gold raises RuntimeError when report status is not SUCCESS."""
        from dags.finance_etl_dag import execute_load_gold

        with patch("src.load_postgres.PostgresDataLoader") as mock_loader_cls:
            mock_instance = MagicMock()
            mock_instance.load_all_target_tickers.return_value = {
                "status": "FAILURE",
                "failed_tickers": [{"ticker": "MSFT", "error": "Database error"}],
            }
            mock_loader_cls.return_value = mock_instance

            with pytest.raises(RuntimeError, match="Gold layer ingestion completed with issues"):
                execute_load_gold()

