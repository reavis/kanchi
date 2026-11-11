import sys
import types
import unittest
from datetime import datetime, timezone, timedelta

sys.modules.setdefault("aiohttp", types.SimpleNamespace(ClientSession=None))

from services.task_service import TaskService
from tests.base import DatabaseTestCase


class TestTaskRoutingInheritance(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        self.service = TaskService(self.session)
        self.base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_inherits_queue_and_routing_key_from_previous_event(self):
        task_id = "task-routing-1"

        self.create_task_event_db(
            task_id=task_id,
            event_type="task-received",
            timestamp=self.base_time,
            routing_key="ws_callback",
            queue="ws_callback",
        )

        success_event = self.create_task_event(
            task_id=task_id,
            event_type="task-succeeded",
            timestamp=self.base_time + timedelta(seconds=5),
            routing_key="default",
            queue=None,
        )

        self.service.save_task_event(success_event)

        saved_events = self.get_task_events_by_id(task_id)
        latest_saved = max(saved_events, key=lambda event: event.timestamp)

        self.assertEqual(success_event.routing_key, "ws_callback")
        self.assertEqual(success_event.queue, "ws_callback")
        self.assertEqual(latest_saved.routing_key, "ws_callback")
        self.assertEqual(latest_saved.queue, "ws_callback")

    def test_preserves_known_queue_when_only_routing_key_is_default(self):
        task_id = "task-routing-2"

        self.create_task_event_db(
            task_id=task_id,
            event_type="task-started",
            timestamp=self.base_time,
            routing_key="priority_jobs",
            queue="priority_jobs",
        )

        success_event = self.create_task_event(
            task_id=task_id,
            event_type="task-succeeded",
            timestamp=self.base_time + timedelta(seconds=10),
            routing_key="default",
            queue="priority_jobs",
        )

        self.service.save_task_event(success_event)

        self.assertEqual(success_event.routing_key, "priority_jobs")
        self.assertEqual(success_event.queue, "priority_jobs")

    def test_keeps_original_values_when_no_prior_routing_metadata_exists(self):
        task_id = "task-routing-3"

        success_event = self.create_task_event(
            task_id=task_id,
            event_type="task-succeeded",
            timestamp=self.base_time,
            routing_key="default",
            queue=None,
        )

        self.service.save_task_event(success_event)

        self.assertEqual(success_event.routing_key, "default")
        self.assertIsNone(success_event.queue)

    def test_inherits_root_and_parent_ids_from_previous_event(self):
        task_id = "task-lineage-1"

        self.create_task_event_db(
            task_id=task_id,
            event_type="task-received",
            timestamp=self.base_time,
            root_id="root-task",
            parent_id="parent-task",
        )

        # Celery omitted lineage, so from_celery_event-style defaults apply.
        success_event = self.create_task_event(
            task_id=task_id,
            event_type="task-succeeded",
            timestamp=self.base_time + timedelta(seconds=5),
            root_id=task_id,
            parent_id=None,
        )

        self.service.save_task_event(success_event)

        saved_events = self.get_task_events_by_id(task_id)
        latest_saved = max(saved_events, key=lambda event: event.timestamp)

        self.assertEqual(success_event.root_id, "root-task")
        self.assertEqual(success_event.parent_id, "parent-task")
        self.assertEqual(latest_saved.root_id, "root-task")
        self.assertEqual(latest_saved.parent_id, "parent-task")

    def test_fills_each_field_from_most_recent_event_that_has_it(self):
        task_id = "task-lineage-2"

        self.create_task_event_db(
            task_id=task_id,
            event_type="task-received",
            timestamp=self.base_time,
            routing_key="ws_callback",
            queue="ws_callback",
            root_id="root-task",
            parent_id="parent-task",
        )
        self.create_task_event_db(
            task_id=task_id,
            event_type="task-started",
            timestamp=self.base_time + timedelta(seconds=1),
            routing_key="default",
            queue="ws_callback",
            root_id=task_id,
            parent_id=None,
        )

        success_event = self.create_task_event(
            task_id=task_id,
            event_type="task-succeeded",
            timestamp=self.base_time + timedelta(seconds=5),
            routing_key="default",
            queue=None,
            root_id=task_id,
            parent_id=None,
        )

        self.service.save_task_event(success_event)

        self.assertEqual(success_event.routing_key, "ws_callback")
        self.assertEqual(success_event.queue, "ws_callback")
        self.assertEqual(success_event.root_id, "root-task")
        self.assertEqual(success_event.parent_id, "parent-task")

    def test_does_not_override_known_root_and_parent_ids(self):
        task_id = "task-lineage-3"

        self.create_task_event_db(
            task_id=task_id,
            event_type="task-received",
            timestamp=self.base_time,
            root_id="old-root",
            parent_id="old-parent",
        )

        success_event = self.create_task_event(
            task_id=task_id,
            event_type="task-succeeded",
            timestamp=self.base_time + timedelta(seconds=5),
            root_id="new-root",
            parent_id="new-parent",
        )

        self.service.save_task_event(success_event)

        self.assertEqual(success_event.root_id, "new-root")
        self.assertEqual(success_event.parent_id, "new-parent")


if __name__ == "__main__":
    unittest.main()
