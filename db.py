import asyncio
from typing import Sequence
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlmodel.ext.asyncio.session import AsyncSession
from models import Job
from sqlmodel import SQLModel, select
import logging

class AsyncDBWorker:
    def __init__(self, mutex: asyncio.Lock):
        self.mutex = mutex
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        # self.session=None
        self.engine:AsyncEngine | None = None

    async def run(self):
        self._running = True
        logging.info("[AsyncDBWorker] Worker started.")
        while self._running:
            try:
                action, job = await self.task_queue.get()
                logging.info(f"[AsyncDBWorker] Received task: {action.upper()} for job: {job}")

                async with AsyncSession(self.engine) as session:
                    if action == "add":
                        session.add(job)
                        await session.commit()
                        await session.refresh(job)
                        logging.info(f"[AsyncDBWorker] Job added to DB: {job}")
                    elif action == "update":
                        await session.merge(job)
                        await session.commit()
                        logging.info(f"[AsyncDBWorker] Job updated in DB: {job}")
                    elif action == "stop":
                        logging.info("[AsyncDBWorker] Received stop signal.")
                        break

            except Exception as e:
                logging.info(f"[AsyncDBWorker] Error during DB operation: {e}")

        logging.info("[AsyncDBWorker] Worker stopping.")

    async def add_job(self, job: Job):
        await self.mutex.acquire()
        try:
            logging.info(f"[AsyncDBWorker] Queuing job for ADD: {job}")
            await self.task_queue.put(("add", job))
        finally:
            self.mutex.release()

    async def update_job(self, job: Job):
        await self.mutex.acquire()
        try:
            logging.info(f"[AsyncDBWorker] Queuing job for UPDATE: {job}")
            await self.task_queue.put(("update", job))
        finally:
            self.mutex.release()

    async def get_job_by_id(self, job_id: str) -> Job | None:
            async with self.mutex:
                async with AsyncSession(self.engine) as session:
                    stmt = select(Job).where(Job.job_id == job_id)
                    result = await session.exec(stmt)
                    return result.first()

    async def get_running_cancellable_jobs(self) -> Sequence[Job]:
        await self.mutex.acquire() 
        try:
            async with AsyncSession(self.engine) as session:
                stmt = select(Job).where(Job.status == "running", Job.cancellable == True)
                result = await session.exec(stmt)
                return result.all()
        finally:
            self.mutex.release()

    async def get_all_jobs(self) -> Sequence[Job]:
        # Return all jobs in the DB      
        async with self.mutex:
            async with AsyncSession(self.engine) as session:
                stmt = select(Job)
                result = await session.exec(stmt)
                return result.all()

    async def stop(self):
        logging.info("[AsyncDBWorker] Stop signal received.")
        self._running = False
        await self.task_queue.put(("stop", None))



    async def init_db_engine(self) -> AsyncEngine:
        # Initializes the SQLite database and returns a session.
        # Only DBWorker shall this session.
        for i in range(10):
            try:
                DATABASE_URL = "postgresql+asyncpg://worker:test@postgres-db/jobs"
                engine:AsyncEngine = create_async_engine(DATABASE_URL, echo=False)
                self.engine = engine

                async with engine.begin() as conn:
                    await conn.run_sync(SQLModel.metadata.create_all)
                return engine

            # Allow postgres to setup                
            except Exception as e:
                logging.info(f"[AsyncDBWorker error : {e}]")
                await asyncio.sleep(1)
