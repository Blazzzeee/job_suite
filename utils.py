import asyncio
import asyncssh
from asyncio import PriorityQueue 
import itertools
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from pydantic import BaseModel
from typing import Optional
from environs import env
import json

MAX_COMMANDS = 5

env.read_env()

raw_instances=env.str("REMOTE_INSTANCES")

try:
    REMOTE_INSTANCES: list[dict] = json.loads(raw_instances)
except json.JSONDecodeError as e:
    print(f"Could not interpret remote instances check your .env file properly {e}")


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
        self.result:str = " "

    def __repr__(self):
        return f"<Job {self.job_id}: {self.command}>"


class JobQueue:
    def __init__(self) :
        self.main_queue=PriorityQueue()
        #TODO retry_queue
        self.retry_queue=PriorityQueue()
        self.counter = itertools.count()
        

    async def enqueue_job(self, job: Job, priority: str, mutex: asyncio.Lock) -> None:
        await mutex.acquire()
        try:
            await self.main_queue.put((PRIORITY_LEVELS[priority], next(self.counter), job))
            print(f"[DEBUG] Item {job} enqueued with priority: {priority}")
        finally:
            mutex.release()

    async def dequeue_job(self, mutex: asyncio.Lock) -> Job | None:
        await mutex.acquire()
        try:
            job_instance = await asyncio.wait_for(self.main_queue.get(), timeout=1)
            return job_instance
        except Exception as e:
            print(f"[Queue Empty] {e}")
            return None
        finally:
            mutex.release()

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


#Connect to remote instances

# REMOTE_INSTANCES = [
#     {"host": "localhost", "user": "blazzee", "password": "blazzee@12", "port":2222},
# ]
#
async def connect_instance(instance):
    try:
        conn = await asyncssh.connect(
            host=instance["host"],
            port=instance.get("port", 22),
            username=instance["user"],
            password=instance.get("password"),  
            known_hosts=None,
        )
        print(f"Connected to {instance['host']}")
        return conn
    except Exception as e:
        print(f"Failed to connect to {instance['host']}: {e}")
        return None

async def connect_remote_instances():
    tasks = [connect_instance(i) for i in REMOTE_INSTANCES]
    connections = await asyncio.gather(*tasks)
    return [conn for conn in connections if conn is not None]


class RemoteWorkerConnection:
    def __init__(self, conn: asyncssh.SSHClientConnection):
        self.conn = conn
        self.max_commands = MAX_COMMANDS
        self.active_commands = 0
        self.lock = asyncio.Lock()
        self.alive = True
        print(f"[INFO] Conneceted to remote worker")

    async def run_command(self, cmd: str, on_line=None, capture_output: bool = True):
        async with self.lock:
            if not self.is_alive():
                raise RuntimeError("Connection is dead")
            if self.active_commands >= self.max_commands:
                raise RuntimeError("Too many commands running")
            self.active_commands += 1
        try:
            process = await self.conn.create_process(cmd)
            if not capture_output:
                # Return the process to the caller for full control
                return " "
            output = ""
            async for line in process.stdout:
                if on_line:
                    await on_line(line.rstrip())
                else:
                    output += line
            await process.wait()
            if process.exit_status != 0:
                raise RuntimeError(f"Command failed with exit code {process.exit_status}: {cmd}")
            return output if not on_line else None
        except (asyncssh.ConnectionLost, BrokenPipeError, OSError):
            self.alive = False
            raise e
        finally:
            async with self.lock:
                self.active_commands -= 1


    # pyright: ignore[reportAttributeAccessIssue]
    def is_alive(self) -> bool:
        return self.conn and not self.conn._transport.is_closing()

    async def close(self):
        self.alive = False
        self.conn.close()
        await self.conn.wait_closed()




class JobDispatcher:
    def __init__(self, job_queue: JobQueue, workers: list[RemoteWorkerConnection], mutex: asyncio.Lock):
        self.job_queue = job_queue
        self.workers = workers
        self.dispatch_interval = 1  # 250ms
        self._running = True
        self.mutex = mutex

    async def dispatch_loop(self):
        while self._running:
            try:
                item = await self.job_queue.dequeue_job(self.mutex)
                if item is None:
                    await asyncio.sleep(self.dispatch_interval)
                    continue

                _, _, job = item 
                worker = await self._get_available_worker()
                if not worker:
                    # Requeue job and wait
                    await self.job_queue.enqueue_job(job, job.priority, self.mutex)
                    await asyncio.sleep(self.dispatch_interval)
                    continue

                asyncio.create_task(self._run_job(worker, job))
            except Exception as e:
                print(f"[Dispatcher] Error: {e}")
                await asyncio.sleep(self.dispatch_interval)


    async def _run_job(self, worker: RemoteWorkerConnection, job: Job):
        try:
            print(f"[Dispatcher] Running job {job.job_id} on {worker.conn._host}")
            output = await worker.run_command(job.command)
            job.result = output or ""
            job.status = "completed"
            print(f"[Dispatcher] Job {job.job_id} completed successfully")
        except Exception as e:
            job.status = "failed"
            job.result = str(e)
            print(f"[Worker Job Error] Job {job.job_id} failed: {e}")

    async def _get_available_worker(self) -> Optional[RemoteWorkerConnection]:
        for worker in self.workers:
            if worker.is_alive() and worker.active_commands < worker.max_commands:
                return worker
        return None

    def stop(self):
        self._running = False
