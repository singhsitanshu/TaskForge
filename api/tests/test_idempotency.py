from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.domain import NewTask
from app.idempotency import idempotency_key_hash, submission_fingerprint


def new_task(**overrides: object) -> NewTask:
    values = {
        "queue": "default",
        "task_type": "test.echo",
        "payload": {"nested": {"alpha": 1, "beta": [1, 2]}, "text": "café"},
        "priority": 5,
        "max_attempts": 3,
        "scheduled_at": None,
        "idempotency_key": "case-sensitive-Key",
    }
    values.update(overrides)
    return NewTask(**values)  # type: ignore[arg-type]


def test_fingerprint_ignores_object_key_order_and_preserves_unicode() -> None:
    first = new_task(
        payload={"é": "東京", "nested": {"a": 1, "b": 2}},
    )
    second = new_task(
        payload={"nested": {"b": 2, "a": 1}, "é": "東京"},
    )
    assert submission_fingerprint(first) == submission_fingerprint(second)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("task_type", "test.other"),
        ("payload", {"nested": {"alpha": 2, "beta": [1, 2]}, "text": "café"}),
        ("priority", 6),
        ("max_attempts", 4),
        ("queue", "other"),
        ("scheduled_at", datetime(2030, 1, 1, tzinfo=UTC)),
    ],
)
def test_semantic_fields_change_fingerprint(field: str, value: object) -> None:
    assert submission_fingerprint(new_task()) != submission_fingerprint(new_task(**{field: value}))


def test_array_order_changes_fingerprint() -> None:
    assert submission_fingerprint(new_task(payload={"items": [1, 2]})) != (
        submission_fingerprint(new_task(payload={"items": [2, 1]}))
    )


def test_idempotency_key_is_not_part_of_semantic_fingerprint() -> None:
    assert submission_fingerprint(new_task(idempotency_key="first")) == (
        submission_fingerprint(new_task(idempotency_key="second"))
    )


def test_equivalent_scheduled_instants_have_same_fingerprint() -> None:
    utc = datetime(2030, 1, 1, 12, tzinfo=UTC)
    offset = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    assert submission_fingerprint(new_task(scheduled_at=utc)) == submission_fingerprint(
        new_task(scheduled_at=offset)
    )


def test_key_hash_is_non_reversible_log_identifier() -> None:
    fingerprint = idempotency_key_hash("sensitive-customer-order")
    assert len(fingerprint) == 16
    assert fingerprint != "sensitive-customer-order"
