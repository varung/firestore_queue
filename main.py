import logging
logging.basicConfig(level=logging.CRITICAL, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

from multiprocessing import Process
from enqueue_tasks import enqueue_tasks
from worker import worker

# setup signal handler to kill all workers on ctrl+c
import signal
import os

# Configure logging
# log module name and level


def main():
    # Step 1: Enqueue tasks
    logging.info("Enqueuing tasks...")
    enqueue_tasks(num_tasks=100)

    # Step 2: Start worker processes
    num_workers = 10
    workers = []
    for i in range(num_workers):
        p = Process(target=worker, args=(i, num_workers))
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
