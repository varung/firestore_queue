import logging
from google.cloud import firestore
from datetime import datetime, timedelta, timezone
import hashlib

# Initialize Firestore client
db = firestore.Client()

# Constants
JOBS_COLLECTION = "jobs"
COMPLETED_COLLECTION = "jobs_completed"
TIMEOUT_MINUTES = 30

# Get a logger for this module
logger = logging.getLogger(__name__)

def create_new_tasks(data_list, num_shards=10):
    """
    Create multiple new tasks with a `shard_key` for sharding.

    Args:
        data_list (list): List of dictionaries, where each dictionary contains task data.
        num_shards (int): Number of shards to distribute tasks across.
    """
    if not data_list:
        logger.warning("No tasks to create. The data list is empty.")
        return

    batch_size = 500
    jobs_ref = db.collection(JOBS_COLLECTION)
    now = datetime.now(timezone.utc)
    initial_update_time = now - timedelta(minutes=TIMEOUT_MINUTES)

    for start in range(0, len(data_list), batch_size):
        batch = db.batch()
        batch_data = data_list[start:start + batch_size]

        for data in batch_data:
            # Generate a unique task ID and compute the shard key
            task_id = jobs_ref.document().id
            shard_key = hash(task_id) % num_shards

            # Prepare the task document
            task_data = {
                "data": data,
                "update": initial_update_time,
                "try_count": 0,
                "shard_key": shard_key
            }
            task_ref = jobs_ref.document(task_id)
            batch.set(task_ref, task_data)

        # Commit the batch
        try:
            batch.commit()
            logger.info(f"Successfully created {len(batch_data)} tasks (Batch start index: {start}).")
        except Exception as e:
            logger.error(f"Error occurred while committing batch starting at index {start}: {e}")


def get_next_available_task(worker_id, num_shards=10, batch_size=10):
    """
    Retrieve the next available task for the worker's shard.

    Args:
        worker_id (int): The unique ID of the worker.
        num_shards (int): Total number of shards.
        batch_size (int): Number of tasks to fetch in the initial query.

    Returns:
        Tuple of (task_id, task_data) if a task is successfully claimed, otherwise (None, None).
    """
    now = datetime.now(timezone.utc)
    timeout_threshold = now - timedelta(minutes=TIMEOUT_MINUTES)
    jobs_ref = db.collection(JOBS_COLLECTION)

    # Determine the shard key for this worker
    shard_key = worker_id % num_shards

    # Query tasks for the worker's shard
    candidate_query = jobs_ref.where(
        filter=firestore.FieldFilter("shard_key", "==", shard_key)
    ).where(
        filter=firestore.FieldFilter("update", "<=", timeout_threshold)
    ).order_by("update").limit(batch_size)

    candidate_tasks = list(candidate_query.stream())

    # Attempt to claim a task transactionally
    for candidate_task in candidate_tasks:
        task_id = candidate_task.id
        task_data = candidate_task.to_dict()

        try:
            def claim_task_transaction(transaction):
                task_ref = jobs_ref.document(task_id)
                snapshot = task_ref.get(transaction=transaction)

                if snapshot.get("update") <= timeout_threshold:
                    transaction.update(task_ref, {
                        "update": now
                    })
                    return task_id, task_data["data"]

                return None, None

            transaction = db.transaction()
            result = claim_task_transaction(transaction)
            if result[0]:
                return result

        except Exception as e:
            logger.error(f"Failed to claim task {task_id}: {e}")
            continue

    return None, None


def mark_task_completed(task_id):
    """
    Mark a task as completed by moving it from the `jobs` collection to the 
    `jobs_completed` collection.

    Args:
        task_id (str): The ID of the task to mark as completed.
    """
    jobs_ref = db.collection(JOBS_COLLECTION).document(task_id)
    completed_jobs_ref = db.collection(COMPLETED_COLLECTION).document(task_id)
    batch = db.batch()

    try:
        # Fetch task data
        task_snapshot = jobs_ref.get()
        if not task_snapshot.exists:
            logger.warning(f"Task {task_id} does not exist.")
            return

        task_data = task_snapshot.to_dict()

        # Add task to `jobs_completed` collection
        batch.set(completed_jobs_ref, {
            **task_data,
            "completed_at": datetime.now(timezone.utc)
        })

        # Delete task from `jobs` collection
        batch.delete(jobs_ref)

        # Commit the batch
        batch.commit()
        logger.info(f"Task {task_id} successfully marked as completed.")

    except Exception as e:
        logger.error(f"Failed to mark task {task_id} as completed: {e}")
