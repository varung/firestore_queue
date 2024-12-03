import logging
from task_manager import create_new_tasks

# Get a logger for this module
logger = logging.getLogger(__name__)

def enqueue_tasks(num_tasks, num_shards):
    """
    Enqueue multiple tasks with shard keys.

    Args:
        num_tasks (int): Number of tasks to create.
        num_shards (int): Number of shards to distribute tasks across.
    """
    tasks = [{"task_number": i, "description": f"Task {i}"} for i in range(num_tasks)]
    logger.info(f"Enqueuing {len(tasks)} tasks across {num_shards} shards.")
    create_new_tasks(tasks, num_shards=num_shards)
