import logging
from multiprocessing import Process
from enqueue_tasks import enqueue_tasks
from worker import worker

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    # Step 1: Enqueue tasks
    logging.info("Enqueuing tasks...")
    enqueue_tasks(num_tasks=1000, num_shards=100)

    # Step 2: Start worker processes
    num_workers = 10
    workers = []
    for i in range(num_workers):
        p = Process(target=worker, args=(i, 10))
        p.start()
        workers.append(p)

    # Step 3: Let workers process tasks
    logging.info("Workers started. Waiting for task completion...")
    for p in workers:
        p.join()


if __name__ == "__main__":
    main()
