import threading
import queue
from models import Job
from sqlmodel import Session


class DBWorker(threading.Thread):
    # A background worker thread dedicated to handling database operations.
    # This class should have zero interaction with any in-memory queue.

    def __init__(self, session: Session):
        super().__init__(daemon=True)
        self.session = session
        self.task_queue = queue.Queue()
        self._stop_event = threading.Event()

    def run(self):
        print("[DBWorker] Worker thread started.")
        while not self._stop_event.is_set():
            try:
                action, job = self.task_queue.get()
                print(f"[DBWorker] Received task: {action.upper()} for job: {job}")

                if action == "add":
                    self.session.add(job)
                    self.session.commit()
                    self.session.refresh(job)
                    print(f"[DBWorker] Job added to DB: {job}")
                elif action == "update":
                    self.session.merge(job)
                    self.session.commit()
                    print(f"[DBWorker] Job updated in DB: {job}")

            except queue.Empty:
                continue #Unreachable
            except Exception as e:
                print(f"[DBWorker] Error during DB operation: {e}")

        print("[DBWorker] Worker thread stopping.")

    def add_job(self, job: Job):
        print(f"[DBWorker] Queuing job for ADD: {job}")
        self.task_queue.put(("add", job))

    def update_job(self, job: Job):
        print(f"[DBWorker] Queuing job for UPDATE: {job}")
        self.task_queue.put(("update", job))

    def stop(self):
        print("[DBWorker] Stop signal received.")
        self._stop_event.set()
