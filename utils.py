from queue import PriorityQueue
import itertools
from sqlmodel import SQLModel, create_engine, Session
from pydantic import BaseModel
from typing import Optional

# Pydantic backed model used for validation of request
class JobRequest(BaseModel):
    name:str
    command: str
    params: Optional[str] = None
    priority: Optional[str] = "medium"
    timeout: Optional[int] = 60
    retries: Optional[int] = 0
    logs: Optional[bool] = False

# Actual job representation
class Job:
    def __init__(self, job_id: str, request: JobRequest):
        self.name = request.name
        self.job_id = job_id
        self.command = request.command
        self.params = request.params
        self.priority = request.priority
        self.timeout = request.timeout
        self.retries = request.retries
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
        self.PRIORITY_LEVELS = {
            "high":0,
            "mid":5,
            "low":10,
        }

    def enqueue_job(self, job:Job, priority):
        #Aquire mutex for buffer write event
        if priority not in ["low", "mid","high"]:
            raise ValueError(f"Invalid priority: {priority}. Must be 'low', 'mid', or 'high'.")
        self.main_queue.put((self.PRIORITY_LEVELS[priority], next(self.counter), job))
        print(f"[DEBUG] Item {job} enqueued with priority:{priority}")
    # Simulated job runner
    def process_job(self, job):
        #Dispatch Jobs to remote instance from here
        pass
    def dequeue_job(self, ):
        #Atomic thread safe dequeue
        pass



DATABASE_URL = "sqlite:///jobs.db"
engine = create_engine(DATABASE_URL, echo=False)

#Depedency Creation
def init_db() -> Session:
    # Initializes the SQLite database and returns a session.
    # Only DBWorker shall this session.
    SQLModel.metadata.create_all(engine)
    session = Session(engine)
    print("[DB] Initialized and session created")
    return session

