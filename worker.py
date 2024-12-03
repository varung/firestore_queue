import logging
import time
import random
from datetime import datetime
from task_manager import get_next_available_task, mark_task_completed, TIMEOUT_MINUTES

# Get a logger for this module
logger = logging.getLogger(__name__)


def worker(worker_id, num_workers, num_shards=10, exit_timeout_factor=2):
    """
    Simulate a worker processing tasks from its shard. Exits after being idle
    for 2 * TIMEOUT_MINUTES.

    Args:
        worker_id (int): The unique ID of the worker.
        num_workers (int): Total number of workers.
        num_shards (int): Total number of shards.
        exit_timeout_factor (int): Factor to multiply TIMEOUT_MINUTES to determine exit timeout.
    """
    max_idle_time = TIMEOUT_MINUTES * exit_timeout_factor * 60  # Convert minutes to seconds
    start_idle_time = None  # Track when the worker starts being idle

    while True:
        try:
            task_id, task_data = get_next_available_task(worker_id, num_workers, num_shards)

            if task_id:
                logger.info(f"Worker {worker_id}: Processing Task {task_id}, Data: {task_data}")

                # Simulate random task failure
                if random.random() < 0.02:  # 20% chance of failure
                    print(f"Worker {worker_id}: Simulated crash on Task {task_id}")
                    raise RuntimeError("Simulated crash")

                # Simulate task processing time
                time.sleep(random.uniform(0.5, 2.0))

                # Mark the task as completed
                mark_task_completed(task_id)
                logger.info(f"Worker {worker_id}: Completed Task {task_id}")

                # Reset idle timer after successful task claim
                start_idle_time = None
            else:
                # No tasks available; start tracking idle time
                if start_idle_time is None:
                    start_idle_time = datetime.now()
                    logger.info(f"Worker {worker_id}: Starting idle timer.")

                elapsed_idle_time = (datetime.now() - start_idle_time).total_seconds()

                if elapsed_idle_time >= max_idle_time:
                    logger.info(f"Worker {worker_id}: Exiting after being idle for {elapsed_idle_time:.2f} seconds.")
                    break

                logger.info(f"Worker {worker_id}: No tasks available. Sleeping for 5 seconds.")
                time.sleep(5)

        except Exception as e:
            logger.error(f"Worker {worker_id}: Error occurred: {e}")
            time.sleep(5)  # Sleep briefly on error
