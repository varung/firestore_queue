import logging
from google.cloud import firestore
from datetime import datetime, timedelta, timezone
# Get a logger for this module
logger = logging.getLogger(__name__)

# Initialize Firestore client
db = firestore.Client()

# conceptually member properties of a queue class
# Constants
prefix = "cloudrun_"
prefix = ""
JOBS_COLLECTION = f"{prefix}jobs"
COMPLETED_COLLECTION = f"{prefix}jobs_completed"
FAILED_COLLECTION = f"{prefix}jobs_failed"

TIMEOUT_MINUTES = .5
NUM_SHARDS = 10
MAX_TRIES = 2


def create_new_tasks(data_list):
    """
    Create multiple new tasks with a `shard_key` for sharding.

    Args:
        data_list (list): List of dictionaries, where each dictionary contains task data.
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
            shard_key = hash(task_id) % NUM_SHARDS

            # Prepare the task document
            task_data = {
                "data": data,
                "update": initial_update_time,
                "try_count": 0,
                "shard_key": shard_key
            }
            logger.info(f"Creating task {task_id} {data} with shard key {shard_key}...")
            task_ref = jobs_ref.document(task_id)
            batch.set(task_ref, task_data)

        # Commit the batch
        try:
            batch.commit()
            logger.info(f"Successfully created {len(batch_data)} tasks (Batch start index: {start}).")
        except Exception as e:
            logger.error(f"Error occurred while committing batch starting at index {start}: {e}")


@firestore.transactional
def claim_task_transaction(transaction, task_id, timeout_threshold):
    """
    Transaction logic to claim a task if it is still available.

    Args:
        transaction: Firestore transaction object.
        task_id (str): The ID of the task to claim.
        timeout_threshold (datetime): The cutoff time for task availability.

    Returns:
        Tuple of (task_id, task_data, reason) if successfully claimed, otherwise (None, None).
    """
    jobs_ref = db.collection(JOBS_COLLECTION)
    task_ref = jobs_ref.document(task_id)
    snapshot = task_ref.get(transaction=transaction)

    if snapshot.exists:
        task_data = snapshot.to_dict()
        if not task_data:  # Empty task data
            return None, None, "EMPTY_TASK_DATA"

        new_try_count = task_data.get("try_count", 0) + 1
        update = task_data.get("update")
        if update <= timeout_threshold:
            now = datetime.now(timezone.utc)

            # Update the task with the new update time and try count
            transaction.update(task_ref, {"update": now, "try_count": new_try_count})

            # If try_count exceeds MAX_TRIES, move the task to the failed collection
            if new_try_count > MAX_TRIES:
                # logger.critical(f"Task {task_id} has excess retries {new_try_count}.")
                failed_jobs_ref = db.collection(FAILED_COLLECTION)
                failed_task_ref = failed_jobs_ref.document(task_id)
                transaction.set(failed_task_ref, {
                    **task_data,
                    "failed_at": now
                })
                transaction.delete(task_ref)
                # logger.critical(f"Task {task_id} {task_data} moved to `{FAILED_COLLECTION}` due to exceeding max retries.")
                return None, None, "EXCESS_RETRIES"
            else:
                return task_id, task_data, None
        else:
            return None, None, f"TASK_LEASED {task_id} {update} > {timeout_threshold}"
    else:
        return None, None, f"NO_SNAPSHOT for {task_id}"


def get_next_available_task(worker_id, num_workers, batch_size=10):
    """
    Retrieve the next available task for the worker. Initially queries assigned shards,
    and defaults to querying all shards if no tasks are found.

    Args:
        worker_id (int): The unique ID of the worker.
        num_workers (int): Total number of workers.
        batch_size (int): Number of tasks to fetch in the initial query.

    Returns:
        Tuple of (task_id, task_data) if a task is successfully claimed, otherwise (None, None).
    """
    now = datetime.now(timezone.utc)
    timeout_threshold = now - timedelta(minutes=TIMEOUT_MINUTES)
    jobs_ref = db.collection(JOBS_COLLECTION)

    # Calculate the worker's assigned shards
    num_shards_per_worker = max(NUM_SHARDS // num_workers, 1)
    assigned_shards = [(worker_id + i * num_workers) % NUM_SHARDS for i in range(num_shards_per_worker)]

    logger.info(f"Worker {worker_id}/{num_workers}: shards: {NUM_SHARDS} => {assigned_shards}")
    if not assigned_shards:
        return None, None

    # Step 1: Query tasks from the worker's assigned shards
    candidate_tasks = []
    for shard_key in assigned_shards:
        shard_query = jobs_ref.where(
            filter=firestore.FieldFilter("shard_key", "==", shard_key)
        ).where(
            filter=firestore.FieldFilter("update", "<=", timeout_threshold)
        ).order_by("update").limit(batch_size)

        candidate_tasks.extend(list(shard_query.stream()))
        # logger.info(f"Worker {worker_id}: Checking shard {shard_key}...{len(candidate_tasks)}")

    # Step 2: If no tasks found, query all shards
    if not candidate_tasks:
        logger.info(f"Worker {worker_id}: No tasks in assigned shards. Checking all shards.")
        all_shard_query = jobs_ref.where(
            filter=firestore.FieldFilter("update", "<=", timeout_threshold)
        ).order_by("update").limit(batch_size)
        candidate_tasks = list(all_shard_query.stream())

    logger.info(f"Worker {worker_id}: Found {len(candidate_tasks)} tasks")
    # Attempt to claim a task transactionally
    for candidate_task in candidate_tasks[:batch_size]:
        task_id = candidate_task.id
        try:
            tid, task, reason = claim_task_transaction(db.transaction(), task_id, timeout_threshold)
            if reason:
                logger.warning(f"Worker {worker_id}: Task {task_id} not claimed: {reason}")
            elif tid:
                return (tid, task)
        except Exception as e:
            logger.error(f"Worker {worker_id}: Failed to claim task {task_id}: {e}")
            continue

    logger.info(f"Worker {worker_id}: No tasks available to claim.")
    return None, None


@firestore.transactional
def mark_task_completed_transaction(transaction, task_id):
    """
    Transaction logic to mark a task as completed by moving it to the `jobs_completed` collection.

    Args:
        transaction: Firestore transaction object.
        task_id (str): The ID of the task to mark as completed.
    """
    jobs_ref = db.collection(JOBS_COLLECTION)
    completed_jobs_ref = db.collection(COMPLETED_COLLECTION)
    task_ref = jobs_ref.document(task_id)
    completed_ref = completed_jobs_ref.document(task_id)

    # Fetch the task document
    snapshot = task_ref.get(transaction=transaction)
    if not snapshot.exists:
        logger.warning(f"Task {task_id} does not exist in the `jobs` collection.")
        return

    task_data = snapshot.to_dict()

    # Move the task to the `jobs_completed` collection
    transaction.set(completed_ref, {
        **task_data,
        "completed_at": datetime.now(timezone.utc)  # Add completion timestamp
    })

    # Delete the task from the `jobs` collection
    transaction.delete(task_ref)

    logger.info(f"Task {task_id} successfully moved to `jobs_completed` and deleted from `jobs`.")



def mark_task_completed(task_id):
    """
    Mark a task as completed by moving it from the `jobs` collection to the `jobs_completed` collection.

    Args:
        task_id (str): The ID of the task to mark as completed.
    """
    try:
        mark_task_completed_transaction(db.transaction(), task_id)
        logger.info(f"Task {task_id} successfully marked as completed.")
    except Exception as e:
        logger.error(f"Failed to mark task {task_id} as completed: {e}")
