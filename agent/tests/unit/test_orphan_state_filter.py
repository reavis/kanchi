import unittest
from datetime import datetime, timezone, timedelta

from database import TaskLatestDB
from services.task_service import TaskService
from tests.base import DatabaseTestCase


class TestOrphanStateFilter(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        self.service = TaskService(self.session)
        self.base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        self._add_latest("running-1", "task-started", offset_seconds=0)
        self._add_latest("orphan-1", "task-started", offset_seconds=1, is_orphan=True)
        self._add_latest("success-1", "task-succeeded", offset_seconds=2)
        self._add_latest("legacy-null", "task-started", offset_seconds=3, is_orphan=None)

    def _add_latest(self, task_id, event_type, offset_seconds=0, is_orphan=False):
        """Helper: insert one row into task_latest."""
        timestamp = self.base_time + timedelta(seconds=offset_seconds)
        self.session.add(TaskLatestDB(
            task_id=task_id,
            event_id=abs(hash(task_id)) % 2_000_000,
            task_name="tasks.example",
            event_type=event_type,
            timestamp=timestamp,
            is_orphan=is_orphan,
            orphaned_at=timestamp if is_orphan else None,
            resolved=False,
        ))
        self.session.commit()

    def _task_ids(self, **kwargs):
        result = self.service.get_recent_events(limit=100, page=0, **kwargs)
        return {event.task_id for event in result["data"]}

    def test_is_orphaned_matches_orphan_flag(self):
        self.assertEqual(self._task_ids(filters="state:is:orphaned"), {"orphan-1"})

    def test_legacy_state_filter_matches_orphan_flag(self):
        self.assertEqual(self._task_ids(filter_state="ORPHANED"), {"orphan-1"})

    def test_is_running_excludes_orphaned_tasks(self):
        self.assertEqual(
            self._task_ids(filters="state:is:running"),
            {"running-1", "legacy-null"}
        )

    def test_not_orphaned_excludes_only_orphaned_tasks(self):
        self.assertEqual(
            self._task_ids(filters="state:not:orphaned"),
            {"running-1", "success-1", "legacy-null"}
        )

    def test_in_combines_orphaned_and_event_states(self):
        self.assertEqual(
            self._task_ids(filters="state:in:orphaned,success"),
            {"orphan-1", "success-1"}
        )

    def test_not_in_excludes_orphaned_and_event_states(self):
        self.assertEqual(
            self._task_ids(filters="state:not_in:orphaned,success"),
            {"running-1", "legacy-null"}
        )

    def test_not_running_keeps_orphaned_tasks(self):
        self.assertEqual(
            self._task_ids(filters="state:not:running"),
            {"orphan-1", "success-1"}
        )


if __name__ == "__main__":
    unittest.main()
