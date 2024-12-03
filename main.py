import logging
from multiprocessing import Process
from enqueue_tasks import enqueue_tasks
from worker import worker
# setup signal handler to kill all workers on ctrl+c
import signal
import os

# Configure logging
# log module name and level
logging.basicConfig(level=logging.CRITICAL, format="%(asctime)s - %(levelname)s - %(name)s\t - %(message)s")


# Number of shards to distribute tasks across
num_shards = 19


def main():
    # Step 1: Enqueue tasks
    logging.info("Enqueuing tasks...")
    enqueue_tasks(num_tasks=100, num_shards=num_shards)

    # Step 2: Start worker processes
    num_workers = 30
    workers = []
    for i in range(num_workers):
        p = Process(target=worker, args=(i, num_workers, num_shards))
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
