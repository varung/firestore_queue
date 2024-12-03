import logging
import time
from task_manager import get_next_available_task, mark_task_completed

# Get a logger for this module
logger = logging.getLogger(__name__)

def worker(worker_id, num_shards=10):
    """
    Simulate a worker processing tasks from its shard.
    
    Args:
        worker_id (int): The unique ID of the worker.
        num_shards (int): Total number of shards.
    """
    while True:
        try:
            task_id, task_data = get_next_available_task(worker_id, num_shards)

            if task_id:
                logger.info(f"Worker {worker_id}: Processing Task {task_id}, Data: {task_data}")

                # Simulate task processing time
                time.sleep(2)

                # Mark the task as completed
                mark_task_completed(task_id)
                logger.info(f"Worker {worker_id}: Completed Task {task_id}")
            else:
                logger.info(f"Worker {worker_id}: No tasks available. Sleeping...")
                time.sleep(5)

        except Exception as e:
            logger.error(f"Worker {worker_id}: Error occurred: {e}")
