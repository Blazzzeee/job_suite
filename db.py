import asyncio
from sqlmodel.ext.asyncio.session import AsyncSession
from models import Job


class AsyncDBWorker:
    def __init__(self, session: AsyncSession):
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
                    await self.session.commit()
                    await self.session.refresh(job)
                    print(f"[AsyncDBWorker] Job added to DB: {job}")
                elif action == "update":
                    await self.session.merge(job)
                    await self.session.commit()
                    print(f"[AsyncDBWorker] Job updated in DB: {job}")
                elif action == "stop":
                    print("[AsyncDBWorker] Received stop signal.")
                    break
            except Exception as e:
                print(f"[AsyncDBWorker] Error during DB operation: {e}")

        print("[AsyncDBWorker] Worker stopping.")

    async def add_job(self, job: Job):
        print(f"[AsyncDBWorker] Queuing job for ADD: {job}")
        await self.task_queue.put(("add", job))

    async def update_job(self, job: Job):
        print(f"[AsyncDBWorker] Queuing job for UPDATE: {job}")
        await self.task_queue.put(("update", job))

    async def get_job_by_id(self, job_id: str) -> Job | None:
        try:
            job = await self.session.get(Job, job_id)
            return job
        except Exception as e:
            print(f"[AsyncDBWorker] Error fetching job {job_id}: {e}")
            return None

    async def stop(self):
        print("[AsyncDBWorker] Stop signal received.")
        self._running = False
        await self.task_queue.put(("stop", None)) 
