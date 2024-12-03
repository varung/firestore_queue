from task_manager import create_new_tasks

def enqueue_tasks(num_tasks=1000, num_shards=10):
    """
    Enqueue multiple tasks with shard keys.
    
    Args:
        num_tasks (int): Number of tasks to create.
        num_shards (int): Number of shards to distribute tasks across.
    """
    tasks = [{"task_number": i, "description": f"Task {i}"} for i in range(num_tasks)]
    create_new_tasks(tasks, num_shards=num_shards)
