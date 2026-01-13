import heapq
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable


# -----------------------------
# Task definition
# -----------------------------
@dataclass(order=True)
class Task:
    run_at: datetime
    priority: int          # lower value = higher priority
    task_id: int
    func: Callable
    retries_left: int
    backoff_seconds: int


# -----------------------------
# Deterministic Scheduler
# -----------------------------
class DeterministicScheduler:
    def __init__(self):
        self._tasks = []

    def add_task(self, task: Task):
        heapq.heappush(self._tasks, task)

    def run(self):
        print("Scheduler started...\n")

        while self._tasks:
            next_task = self._tasks[0]   # peek, do not pop yet
            now = datetime.now()

            if next_task.run_at <= now:
                task = heapq.heappop(self._tasks)

                try:
                    print(f"[{datetime.now().time()}] Running Task {task.task_id}")
                    task.func()
                    print(f"Task {task.task_id} completed successfully\n")

                except Exception as e:
                    print(f"Task {task.task_id} failed: {e}")

                    if task.retries_left > 0:
                        task.retries_left -= 1
                        task.run_at = datetime.now() + timedelta(seconds=task.backoff_seconds)

                        print(
                            f"Retrying Task {task.task_id} "
                            f"in {task.backoff_seconds}s "
                            f"(retries left: {task.retries_left})\n"
                        )

                        heapq.heappush(self._tasks, task)
                    else:
                        print(f"Task {task.task_id} permanently failed\n")
            else:
                time.sleep(0.5)

        print("Scheduler finished. No more tasks.")


# -----------------------------
# Example tasks
# -----------------------------
def task_success():
    print("This task always succeeds")


def task_fail_once():
    print("This task fails")
    raise ValueError("Intentional failure")


# -----------------------------
# Main execution
# -----------------------------
if __name__ == "__main__":
    scheduler = DeterministicScheduler()
    now = datetime.now()

    scheduler.add_task(
        Task(
            run_at=now + timedelta(seconds=2),
            priority=-1,
            task_id=1,
            func=task_success,
            retries_left=0,
            backoff_seconds=0
        )
    )

    scheduler.add_task(
        Task(
            run_at=now + timedelta(seconds=2),
            priority=-1,
            task_id=2,
            func=task_fail_once,
            retries_left=2,
            backoff_seconds=3
        )
    )

    scheduler.add_task(
        Task(
            run_at=now + timedelta(seconds=5),
            priority=-2,
            task_id=3,
            func=task_success,
            retries_left=0,
            backoff_seconds=0
        )
    )

    scheduler.run()
