import logging
from multiprocessing import Process
from enqueue_tasks import enqueue_tasks
from worker import worker
from task_manager import NUM_SHARDS
# setup signal handler to kill all workers on ctrl+c
import signal
import os

# Configure logging
# log module name and level
logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(name)s\t - %(message)s")


def main():
    # Step 1: Enqueue tasks
    logging.info("Enqueuing tasks...")
    enqueue_tasks(num_tasks=100, num_shards=NUM_SHARDS)

    # Step 2: Start worker processes
    num_workers = 10
    workers = []
    for i in range(num_workers):
        p = Process(target=worker, args=(i, num_workers, NUM_SHARDS))
        p.start()
        workers.append(p)

    def signal_handler(sig, frame):
        logging.info("Ctrl+C pressed. Terminating workers...")
        for p in workers:
            os.kill(p.pid, signal.SIGTERM)

    signal.signal(signal.SIGINT, signal_handler)

    # Step 3: Let workers process tasks
    logging.info("Workers started. Waiting for task completion...")
    for p in workers:
        p.join()


if __name__ == "__main__":
    main()
