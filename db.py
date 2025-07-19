import asyncio
from sqlmodel import Session
from models import Job


class AsyncDBWorker:
    def __init__(self, session: Session):
        self.session = session
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self._running = False

    async def run(self):
        self._running = True
        print("[AsyncDBWorker] Worker started.")
        while self._running:
            try:
                action, job = await self.task_queue.get()
                print(f"[AsyncDBWorker] Received task: {action.upper()} for job: {job}")

                if action == "add":
                    self.session.add(job)
                    self.session.commit()
                    self.session.refresh(job)
                    print(f"[AsyncDBWorker] Job added to DB: {job}")
                elif action == "update":
                    self.session.merge(job)
                    self.session.commit()
                    print(f"[AsyncDBWorker] Job updated in DB: {job}")
            except Exception as e:
                print(f"[AsyncDBWorker] Error during DB operation: {e}")

        print("[AsyncDBWorker] Worker stopping.")

    async def add_job(self, job: Job):
        print(f"[AsyncDBWorker] Queuing job for ADD: {job}")
        await self.task_queue.put(("add", job))

    async def update_job(self, job: Job):
        print(f"[AsyncDBWorker] Queuing job for UPDATE: {job}")
        await self.task_queue.put(("update", job))

    def stop(self):
        print("[AsyncDBWorker] Stop signal received.")
        self._running = False
