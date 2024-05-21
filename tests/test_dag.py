import unittest
from unittest.mock import patch
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from datetime import datetime
import pytest


# Testes de estrutura da DAG
class TestChoreDagStructure(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag()

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id="chore_dag")
        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, "chore_dag")

    def test_task_count(self):
        dag = self.dagbag.get_dag(dag_id="chore_dag")
        self.assertEqual(len(dag.tasks), 5)

    def test_task_dependencies(self):
        dag = self.dagbag.get_dag(dag_id="chore_dag")
        dependencies = {
            "task1": ["task2"],
            "task2": ["task3"],
            "task3": ["task4"],
            "task4": ["task5"],
        }
        for upstream_task, downstream_tasks in dependencies.items():
            for downstream_task in downstream_tasks:
                self.assertIn(
                    downstream_task,
                    [t.task_id for t in dag.get_task(upstream_task).downstream_list],
                )


# Teste de execução de tarefas da DAG
@pytest.mark.parametrize("task_id", ["task1", "task2", "task3", "task4", "task5"])
def test_task_execution(dag_bag, task_id):
    dag = dag_bag.get_dag(dag_id="chore_dag")
    task = dag.get_task(task_id=task_id)
    ti = TaskInstance(task=task, execution_date=datetime.now())
    ti.run()
    assert ti.state == State.SUCCESS


# Teste de integração de ponta a ponta
def test_dag_run(dag_bag):
    dag = dag_bag.get_dag(dag_id="chore_dag")
    dagrun = dag.create_dagrun(
        run_id="test_run",
        state=State.RUNNING,
        execution_date=datetime.now(),
        start_date=datetime.now(),
    )
    dagrun.run()
    assert dagrun.state == State.SUCCESS


# Executar os testes unittest
if __name__ == "__main__":
    unittest.main()
