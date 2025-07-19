import asyncio
from queue import PriorityQueue, Empty
import itertools
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from pydantic import BaseModel
from typing import Optional

#Constants
PRIORITY_LEVELS = {
            "high":0,
            "mid":5,
            "low":10,
        }

# Pydantic backed model used for validation of request
class JobRequest(BaseModel):
    name:str
    command: str
    params: Optional[str] = None
    priority: Optional[str] = "med"
    timeout: Optional[int] = 60
    retries: Optional[int] = 0
    logs: Optional[bool] = False

# Actual job representation
class Job:
    def __init__(self, job_id: str, request: JobRequest):
        self.name = request.name
        self.job_id = job_id
        if request.command.strip() == "": 
            raise ValueError("Illegal Argument The command cannot be a empty string")
        else:
            self.command = request.command
        self.params = request.params
        if request.priority in PRIORITY_LEVELS.keys():
            self.priority = request.priority
        else:
            raise ValueError("Illegal Argument passed as priority must be low, mid or high")
        self.timeout = request.timeout
        if request.retries != None and request.retries >= 0:
            self.retries = request.retries
        else:
            raise ValueError(f"Illegal Argument passed in retries , must be greater than 0 , passed {request.retries}")
        self.logs = request.logs
        self.status = "queued"
        self.result = None

    def __repr__(self):
        return f"<Job {self.job_id}: {self.command}>"


class Queue:
    def __init__(self) :
        self.main_queue=PriorityQueue()
        self.retry_queue=PriorityQueue()

        self.counter = itertools.count()
        

    async def enqueue_job(self, job:Job, priority, mutex:asyncio.Lock) -> None:
        await mutex.acquire()
        #Aquire mutex for save buffer write
        try:
            #TODO: Deal with queue.put(block)
            self.main_queue.put((PRIORITY_LEVELS[priority], next(self.counter), job))
            print(f"[DEBUG] Item {job} enqueued with priority:{priority}")
        finally:
            mutex.release()

    # Simulated job runner
    def process_job(self, job):
        #Dispatch Jobs to remote instance from here
        pass


    async def dequeue_job(self, mutex:asyncio.Lock) -> Job | None:
        #Atomic thread safe dequeue
        await mutex.acquire()
        #Aquire mutex for safe dequeue 
        try:
            job_instance=self.main_queue.get(block=False)
        except Empty:
            print(f"[INFO] Skipping Dequeue operation (Underflow)")
            job_instance=None 
        finally:
            mutex.release()
        return job_instance


#Depedency Creation
async def init_db() -> AsyncSession:
    # Initializes the SQLite database and returns a session.
    # Only DBWorker shall this session.
    DATABASE_URL = "sqlite+aiosqlite:///jobs.db"  
    engine:AsyncEngine = create_async_engine(DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    session = AsyncSession(engine)
    print("[DB] Initialized and async session created")
    return session
