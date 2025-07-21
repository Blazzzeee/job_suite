import asyncio
from sqlmodel.ext.asyncio.session import AsyncSession
from models import Job
from sqlmodel import select
from typing import List

class AsyncDBWorker:
    def __init__(self, session: AsyncSession, mutex: asyncio.Lock):
        self.session = session
        self.mutex = mutex
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
        await self.mutex.acquire()
        try:
            print(f"[AsyncDBWorker] Queuing job for ADD: {job}")
            await self.task_queue.put(("add", job))
        finally:
            self.mutex.release()

    async def update_job(self, job: Job):
        await self.mutex.acquire()
        try:
            print(f"[AsyncDBWorker] Queuing job for UPDATE: {job}")
            await self.task_queue.put(("update", job))
        finally:
            self.mutex.release()

    async def get_job_by_id(self, job_id: str) -> Job | None:
            async with self.mutex:
                stmt = select(Job).where(Job.job_id == job_id)
                result = await self.session.exec(stmt)
                return result.first()

    async def get_all_jobs(self) -> List[Job]:
        # Return all jobs in the DB            async with self.mutex:
        async with self.mutex:
                stmt = select(Job)
                result = await self.session.exec(stmt)
                return result.all()

    async def stop(self):
        print("[AsyncDBWorker] Stop signal received.")
        self._running = False
        await self.task_queue.put(("stop", None))
