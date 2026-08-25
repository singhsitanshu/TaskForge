import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest
from app.config import HeartbeatSettings
from app.main import create_app
from fastapi.testclient import TestClient
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "migrations"
UP_SQL = "\n".join(path.read_text() for path in sorted(MIGRATIONS.glob("*.up.sql")))
DOWN_SQL = "\n".join(
    path.read_text() for path in sorted(MIGRATIONS.glob("*.down.sql"), reverse=True)
)
HEARTBEAT_INTERVAL = 0.1
STALE_AFTER = 0.3
DEAD_AFTER = 0.7
SETTINGS = HeartbeatSettings(
    interval=timedelta(seconds=HEARTBEAT_INTERVAL),
    stale_after=timedelta(seconds=STALE_AFTER),
    dead_after=timedelta(seconds=DEAD_AFTER),
)


@pytest.fixture
def heartbeat_environment() -> Iterator[tuple[str, TestClient]]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for heartbeat tests")

    schema_name = f"heartbeat_e2e_{uuid.uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
        )
        try:
            connection.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
            )
            connection.execute(UP_SQL)
            application = create_app(
                database_url,
                database_connection_kwargs={"options": f"-csearch_path={schema_name}"},
                heartbeat_settings=SETTINGS,
            )
            with TestClient(application) as client:
                yield schema_name, client
        finally:
            connection.execute(DOWN_SQL)
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )


def schema_connection(schema_name: str) -> psycopg.Connection:
    return psycopg.connect(
        os.environ["TEST_DATABASE_URL"],
        autocommit=True,
        options=f"-csearch_path={schema_name}",
    )


def start_worker(schema_name: str, instance_id: str) -> subprocess.Popen:
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": os.environ["TEST_DATABASE_URL"],
            "PGOPTIONS": f"-csearch_path={schema_name}",
            "WORKER_ID": instance_id,
            "WORKER_NAME": instance_id,
            "POLL_INTERVAL": "20ms",
            "WORKER_HEARTBEAT_INTERVAL": "100ms",
            "WORKER_STALE_AFTER": "300ms",
            "WORKER_DEAD_AFTER": "700ms",
            "WORKER_HEARTBEAT_TIMEOUT": "50ms",
            "HTTP_ADDR": "127.0.0.1:0",
        }
    )
    return subprocess.Popen(
        ["taskforge-worker"],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def stop_worker(process: subprocess.Popen) -> str:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    assert process.stdout is not None
    return process.stdout.read()


def worker_id(schema_name: str, instance_id: str, process: subprocess.Popen) -> str:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"worker exited before registration:\n{stop_worker(process)}")
        with schema_connection(schema_name) as connection:
            row = connection.execute(
                "SELECT id::text FROM workers WHERE instance_id = %s",
                (instance_id,),
            ).fetchone()
        if row:
            return row[0]
        time.sleep(0.02)
    pytest.fail(f"worker {instance_id} did not register")


def wait_for_task_status(
    client: TestClient,
    task_id: str,
    status: str,
    process: subprocess.Popen,
    timeout: float = 5,
) -> dict:
    deadline = time.monotonic() + timeout
    last_task = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(
                f"worker exited while waiting for task:\n{stop_worker(process)}"
            )
        response = client.get(f"/tasks/{task_id}")
        assert response.status_code == 200
        last_task = response.json()
        if last_task["status"] == status:
            return last_task
        time.sleep(0.02)
    pytest.fail(f"task did not reach {status}; last state={last_task}")


def wait_for_liveness(
    client: TestClient,
    worker_id_value: str,
    state: str,
    timeout: float = 3,
) -> dict:
    deadline = time.monotonic() + timeout
    last_worker = None
    while time.monotonic() < deadline:
        response = client.get(f"/workers/{worker_id_value}")
        assert response.status_code == 200
        last_worker = response.json()
        if last_worker["liveness"] == state:
            return last_worker
        time.sleep(0.02)
    pytest.fail(f"worker did not reach {state}; last state={last_worker}")


def read_last_heartbeat(schema_name: str, instance_id: str):
    with schema_connection(schema_name) as connection:
        return connection.execute(
            "SELECT last_seen_at FROM workers WHERE instance_id = %s",
            (instance_id,),
        ).fetchone()[0]


def test_heartbeats_continue_during_long_running_task(
    heartbeat_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = heartbeat_environment
    instance_id = f"long-task-{uuid.uuid4()}"
    process = start_worker(schema_name, instance_id)
    output = ""
    try:
        registered_worker_id = worker_id(schema_name, instance_id, process)
        response = client.post(
            "/tasks",
            json={"task_type": "test.sleep", "payload": {"duration_ms": 900}},
        )
        assert response.status_code == 201
        task_id = response.json()["id"]
        wait_for_task_status(client, task_id, "RUNNING", process)

        observed_heartbeats = {read_last_heartbeat(schema_name, instance_id)}
        while True:
            task = client.get(f"/tasks/{task_id}").json()
            worker = client.get(f"/workers/{registered_worker_id}").json()
            observed_heartbeats.add(read_last_heartbeat(schema_name, instance_id))
            assert worker["liveness"] == "ACTIVE"
            if task["status"] == "SUCCEEDED":
                break
            time.sleep(0.04)

        assert task["result"] == {"slept_ms": 900}
        assert len(observed_heartbeats) >= 5
        print(
            "LONG_TASK duration_ms=900 interval_ms=100 "
            f"heartbeats_observed={len(observed_heartbeats)} liveness=ACTIVE"
        )
    finally:
        output = stop_worker(process)
        assert process.returncode == 0, output


def test_stopped_worker_transitions_without_task_recovery(
    heartbeat_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = heartbeat_environment
    instance_id = f"stopped-{uuid.uuid4()}"
    process = start_worker(schema_name, instance_id)
    registered_worker_id = worker_id(schema_name, instance_id, process)
    response = client.post(
        "/tasks",
        json={"task_type": "test.sleep", "payload": {"duration_ms": 2000}},
    )
    task_id = response.json()["id"]
    wait_for_task_status(client, task_id, "RUNNING", process)
    wait_for_liveness(client, registered_worker_id, "ACTIVE")

    output = stop_worker(process)
    assert process.returncode == 0, output
    stopped_at = time.monotonic()
    stale = wait_for_liveness(client, registered_worker_id, "STALE")
    stale_elapsed = time.monotonic() - stopped_at
    dead = wait_for_liveness(client, registered_worker_id, "DEAD")
    dead_elapsed = time.monotonic() - stopped_at
    assert stale["heartbeat_age_seconds"] > STALE_AFTER
    assert dead["heartbeat_age_seconds"] > DEAD_AFTER

    with schema_connection(schema_name) as connection:
        task_state = connection.execute(
            "SELECT status::text, attempt_count FROM tasks WHERE id = %s",
            (task_id,),
        ).fetchone()
        attempt_state = connection.execute(
            "SELECT status::text, finished_at FROM task_attempts WHERE task_id = %s",
            (task_id,),
        ).fetchone()
    assert task_state == ("RUNNING", 1)
    assert attempt_state == ("RUNNING", None)
    print(
        "WORKER_STOP active=true "
        f"stale_after_seconds={stale_elapsed:.3f} dead_after_seconds={dead_elapsed:.3f} "
        "task_state=RUNNING attempt_state=RUNNING"
    )


def test_multiple_workers_heartbeat_independently(
    heartbeat_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = heartbeat_environment
    instance_ids = [f"multi-{index}-{uuid.uuid4()}" for index in range(3)]
    processes = [start_worker(schema_name, instance_id) for instance_id in instance_ids]
    stopped = set()
    try:
        worker_ids = {
            instance_id: worker_id(schema_name, instance_id, process)
            for instance_id, process in zip(instance_ids, processes, strict=True)
        }
        observations = {instance_id: set() for instance_id in instance_ids}
        deadline = time.monotonic() + 0.55
        while time.monotonic() < deadline:
            for instance_id in instance_ids:
                observations[instance_id].add(
                    read_last_heartbeat(schema_name, instance_id)
                )
                worker = client.get(f"/workers/{worker_ids[instance_id]}").json()
                assert worker["liveness"] == "ACTIVE"
            time.sleep(0.04)

        output = stop_worker(processes[0])
        stopped.add(0)
        assert processes[0].returncode == 0, output
        wait_for_liveness(client, worker_ids[instance_ids[0]], "STALE")
        wait_for_liveness(client, worker_ids[instance_ids[0]], "DEAD")
        final_states = {}
        for index, instance_id in enumerate(instance_ids):
            worker = client.get(f"/workers/{worker_ids[instance_id]}").json()
            final_states[instance_id] = worker["liveness"]
            expected = "DEAD" if index == 0 else "ACTIVE"
            assert worker["liveness"] == expected
            assert len(observations[instance_id]) >= 3
            print(
                f"MULTI_WORKER worker={index} "
                f"heartbeats_observed={len(observations[instance_id])} "
                f"final_liveness={expected}"
            )

        assert len(set(worker_ids.values())) == 3
        assert final_states[instance_ids[0]] == "DEAD"
    finally:
        for index, process in enumerate(processes):
            if index not in stopped:
                stop_worker(process)
            assert process.returncode == 0
