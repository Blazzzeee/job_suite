import asyncio
import asyncssh
from asyncio import PriorityQueue 
import itertools
from pydantic import BaseModel
from typing import Optional
from environs import env
import json
import db
import datetime
from fastapi import WebSocket
import sys
import logging

#Constants
MAX_COMMANDS = 100
env.read_env()
raw_instances=env.str("REMOTE_INSTANCES")
try:
    REMOTE_INSTANCES: list[dict] = json.loads(raw_instances)
except json.JSONDecodeError as e:
    logging.info(f"Could not interpret remote instances check your .env file properly {e}")
PRIORITY_LEVELS = {
            "high":0,
            "mid":5,
            "low":10,
        }
DISPATCH_INTERVAL=1

# Pydantic backed model used for validation of request
class JobRequest(BaseModel):
    name:str
    command: str
    params: Optional[str] = None
    priority: Optional[str] = "med"
    timeout: Optional[int] = 60
    retries: Optional[int] = 0
    logs: Optional[bool] = False
    cancellable:bool = False

# Actual job representation in-memory
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
        self.cancelable:bool=request.cancellable

    def __repr__(self):
        return f"<Job {self.job_id}: {self.command}>"


class JobQueue:
    #The job queue is an async safe queue used (in-memory) for enqueue and deuque operations 
    #The dispatcher has depedency upon this queue to get what task to execute
    def __init__(self, mutex:asyncio.Lock) :
        self.main_queue=PriorityQueue()
        #TODO retry_queue
        self.retry_queue=PriorityQueue()
        self.counter = itertools.count()
        self.mutex = mutex 

    async def enqueue_job(self, job: Job, priority: str) -> None:
        #this ensures the job is queued without any race conditons
        await self.mutex.acquire()
        try:
            await self.main_queue.put((PRIORITY_LEVELS[priority], next(self.counter), job))
            logging.info(f"[DEBUG] Item {job} enqueued with priority: {priority}")
        finally:
            self.mutex.release()

    async def dequeue_job(self) -> Job | None:
        #this ensures job is dequeued without any race conditons
        await self.mutex.acquire()
        try:
            job_instance = await asyncio.wait_for(self.main_queue.get(), timeout=1)
            return job_instance
        except Exception as e:
            logging.info(f"[Queue Empty] {e}")
            return None
        finally:
            self.mutex.release()


async def connect_instance(instance):
    #This method initiates a ssh connection to a remote instance specified as {instance}
    try:
        conn = await asyncssh.connect(
            host=instance["host"],
            port=instance.get("port", 22),
            username=instance["user"],
            password=instance.get("password"),  
            known_hosts=None,
        )
        logging.info(f"[INSTANCE] Connected to {instance['host']}")
        return conn
    except Exception as e:
        logging.info(f"[INSTANCE] Failed to connect to {instance['host']}: {e}")
        return None

async def connect_remote_instances():
    #Goes over all remote instances defined in global REMOTE_INSTANCES
    tasks = [connect_instance(i) for i in REMOTE_INSTANCES]
    connections = await asyncio.gather(*tasks)
    return [conn for conn in connections if conn is not None]


class RemoteWorkerConnection:
    #This is a wrapper class around a ssh connection to add extra functionality like running commands , keeping track of command , upper bund on commands and a alive state
    def __init__(self, conn: asyncssh.SSHClientConnection):
        self.conn = conn
        self.max_commands = MAX_COMMANDS
        self.active_commands = 0
        self.lock = asyncio.Lock()
        self.alive = True

        logging.info(f"[INFO] Conneceted to remote worker")

    async def run_command(self, cmd: str, on_line=None, capture_output: bool = False):
        #This command is used by Dispatcher to dispatch shell compatible commands, it allows access to real time logs via WebSocket
        async with self.lock:
            if not self.is_alive():
                raise RuntimeError("Connection is dead")
            if self.active_commands >= self.max_commands:
                raise RuntimeError("Too many commands running")
            self.active_commands += 1
        try:
            process = await self.conn.create_process(cmd)
            if not capture_output:
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

        except Exception as e:
            logging.error(e)
            self.alive = False
            await connect_remote_instances()
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
    #The Job dispatcher is responsible for all dispatches to remote workers , the job dispatcher actively monitor the job queue(in-memory)
    #If there jobs available and a remote worker is avaliable , the job is dispatched and state info is updated via db_worker
    def __init__(self, job_queue: JobQueue, workers: list[RemoteWorkerConnection], db_worker:db.AsyncDBWorker ):
        self.job_queue = job_queue
        self.workers = workers
        self.dispatch_interval = DISPATCH_INTERVAL 
        self._running = True
        self.mutex = job_queue.mutex
        self.db_worker = db_worker
        self.running_tasks: dict[str, asyncio.Task] = {}
        self.log_streams: dict[str, list[WebSocket]] = {}
        self.log_buffers: dict[str, list[str]] = {}

    async def dispatch_loop(self):
        while self._running:
            try:
                # Find an worker
                worker = await self._get_available_worker()
                if not worker:
                    await asyncio.sleep(self.dispatch_interval)
                    continue

                # dequeue if there was a worker
                item = await self.job_queue.dequeue_job()
                if item is None:
                    await asyncio.sleep(self.dispatch_interval)
                    continue

                _, _, job = item

                # DB lookup
                db_job = None
                while db_job is None:
                    db_job = await self.db_worker.get_job_by_id(job.job_id)
                    if db_job is None:
                        logging.info(f"[DEBUG] Job could not be found in db")
                        await asyncio.sleep(0.2)
                        continue

                sys.stdout.flush()
                #Dispatch job via asyncssh
                asyncio.create_task(self._run_job(worker, db_job))

            except Exception as e:
                logging.info(f"[Dispatcher] Error: {e}")
                await asyncio.sleep(self.dispatch_interval)


    async def _run_job(self, worker: RemoteWorkerConnection, db_job: db.Job):
        job_id = db_job.job_id
        start_time = datetime.datetime.now()
        full_command = f"{db_job.command} {db_job.params}" if db_job.params else db_job.command

        # Set up log streaming if enabled
        async def stream_log_line(line: str):
            self.log_buffers[job_id].append(line)
            clients = self.log_streams.get(job_id, [])
            for ws in clients:
                try:
                    await ws.send_text(line)
                except Exception:
                    pass  # TODO: Clean up disconnected clients

        on_line = stream_log_line if db_job.logs else None
        capture_output = True

        # Create the actual task
        cmd_task = asyncio.create_task(
            worker.run_command(full_command, on_line=on_line, capture_output=capture_output)
        )
        self.running_tasks[job_id] = cmd_task

        # Mark "running" if still alive after 0.5s
        done, pending = await asyncio.wait({cmd_task}, timeout=0.5)
        if cmd_task in pending:
            db_job.status = "running"
            db_job.started_at = start_time
            await self.db_worker.update_job(db_job)
            logging.info(f"[Dispatcher] Job {job_id} running (took >0.5s)")

        try:
            # ENFORCE TIMEOUT
            output = await asyncio.wait_for(cmd_task, timeout=db_job.timeout)
            db_job.result = output or ""
            db_job.status = "completed"
        except asyncio.TimeoutError:
            cmd_task.cancel()
            db_job.result = "Job timed out"
            db_job.status = "timeout"
            logging.info(f"[Dispatcher] Job {job_id} timed out after {db_job.timeout}s")
        except Exception as e:
            db_job.result = str(e)
            db_job.status = "failed"
        finally:
            self.running_tasks.pop(job_id, None)
            db_job.finished_at = datetime.datetime.now()
            db_job.updated_at = datetime.datetime.now()
            await self.db_worker.update_job(db_job)
            logging.info(f"[Dispatcher] Job {job_id} done: status={db_job.status}")


    async def _get_available_worker(self) -> Optional[RemoteWorkerConnection]:
        #Finds a worker that is alive and has available limit for atleast 1 more command
        for worker in self.workers:
            if worker.is_alive() and worker.active_commands < worker.max_commands:
                return worker
        return None

    async def cancel_job(self, job_id: str) -> bool:
        # Tell the remote client to cancel the job
        task = self.running_tasks.get(job_id)
        if task and not task.done():
            task.cancel()
            try:
                await task  
            except asyncio.CancelledError:
                logging.info(f"[Dispatcher] Job {job_id} cancelled")
            return True
        return False

    def stop(self):
        self._running = False



    async def gracefull_shutdown(self):
        # query all the running jobs , which are cancellable 
        # Dispatch a cancel event to remote instance
        # Wait for every task to get cancelled

        logging.info("[DISPATCHER] Graceful shutdown initiated...")
        for job_id, task in list(self.running_tasks.items()):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logging.info(f"[DISPATCHER] Job cancelled due to shutdown")
                    db_job = await self.db_worker.get_job_by_id(job_id)
                    if db_job:
                        db_job.status = "cancelled"
                        db_job.result = "Job cancelled due to shutdown"
                        db_job.finished_at = datetime.datetime.now()
                        db_job.updated_at = datetime.datetime.now()
                        await self.db_worker.update_job(db_job)
                    return True
                return False

        
