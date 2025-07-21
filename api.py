from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import uuid
import uvicorn
import utils 
import models
import db
import asyncio

db_worker = None
remote_workers = []
queue = None
queue_mutex=None
dispatcher = None
db_mutex = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_worker,remote_workers, queue ,queue_mutex,dispatcher
    # STARTUP

    #In memory task queue (PriorityQueue)
    queue_mutex = asyncio.Lock()
    queue = utils.JobQueue(queue_mutex)

    #Depedency creation
    session = await utils.init_db()
    
    #The db_mutex safeguards all writes to database file and is used in every read/write operation involving db
    db_mutex = asyncio.Lock()
    #The db worker is responsible for all operations involving database updates
    db_worker = db.AsyncDBWorker(session=session, mutex=db_mutex)
    #The worker task is actual async task , in which db_worker operations are executed from
    worker_task = asyncio.create_task(db_worker.run())
    print("[INFO] Database worker started")

    #This routine connects to all remote instances that are defined in an env file and return successfull connections in a list
    connections = await utils.connect_remote_instances()

    #A wrapper class to manage mutex for tasks , connection state , tasks alloted and more
    for conn in connections:
        try:
            remote_conn=utils.RemoteWorkerConnection(conn)
            remote_workers.append(remote_conn) 
        except Exception as e:
            print(e)

    #The dispatcher performs dequeues and allots task to remote instances 
    dispatcher=utils.JobDispatcher(job_queue=queue, workers=remote_workers, db_worker=db_worker)
    dispatcher_task = asyncio.create_task(dispatcher.dispatch_loop())

    yield 

    # SHUTDOWN
    if db_worker:
        #terminate database worker
        await db_worker.stop()
        print("[INFO] Database worker stopped")
    #Terminate asuny task for db_wroker
    worker_task.cancel()
    for worker in remote_workers:
        try:
            await worker.close()
            #Nicely free up remote workers
            print("[INFO] Disconnected from remote worker")
        except Exception as e:
            print(f"[WARN] Failed to close remote connection: {e}")

    dispatcher.stop()
    print(f"[INFO] Stopping dispatcher")
    await asyncio.sleep(0.3) 


app = FastAPI(lifespan=lifespan)

MAX_RETRIES = 10


# # TEMP: Simulate db  REPLACED
jobs = {}

# Submit a job for execution
@app.post("/jobs/", status_code=201)
async def post_job(job: utils.JobRequest):
    #Unique refernce used for cross mapping of memory and database
    job_id = str(uuid.uuid4())
    #The job instance is the in memory validated job_request
    job_instance = utils.Job(job_id, job)

    try: 
        #job_db_instance is the Job model used for persitent storage
        db_job_instance = models.Job(
                job_id=str(job_id),
                name=job.name,
                command=job.command,
                params=job.params,
                priority=job.priority,
                timeout=job.timeout,
                status="queued",
            )
    except Exception as e:
        #Occurs if any field is invalid
        print(f"Recived invalid request {e}")
        raise HTTPException(status_code=400, detail="Invalid argument (invalid request payload)") 

    try:
        if queue!=None and db_worker!=None and queue_mutex!=None:
            #thread safe | async safe enqueu with mutexes
            await db_worker.add_job(db_job_instance)
            await queue.enqueue_job(job_instance, job_instance.priority)
    except ValueError as e:
        print(f"[DEBUG]Cannot enqueue job .. skipping ... {e}")
    except Exception as e:
        print(f"[ERROR]Unknown error occurred")

    print(f"[DEBUG] New job queued {job_instance}")

    return {
        "status": "submitted",
        "job_id": job_id,
        "command": job.command,
        "priority": job.priority
    }


# Get current state of job using job id
@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    if db_worker != None:
        job = await db_worker.get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        return {
            "job_id": job.job_id,
            "command": job.command,
            "params": job.params,
            "priority": job.priority,
            "timeout": job.timeout,
            "status": job.status,
            "result": job.result,
            "started_at":job.started_at,
            "finished_at": job.finished_at
        }

# Get list of all running jobs
@app.get("/jobs/")
async def list_jobs():
    if db_worker!=None:
        jobs = await db_worker.get_all_jobs()
        
        response=list()
        for job in jobs:
            temp = {
            "job_id": job.job_id,
            "command": job.command,
            "params": job.params,
            "priority": job.priority,
            "timeout": job.timeout,
            "status": job.status,
            "result": job.result,
            "started_at":job.started_at,
            "finished_at": job.finished_at
        }
            response.append(job)

        return response

    
# Cancel a running job
@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    db_job=None
    if db_worker!=None:
        db_job = await db_worker.get_job_by_id(job_id)
    if  not db_job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if db_job.status in ["completed", "failed"]:
        return {"status": "cannot cancel", "reason": f"job already {job.status}"}

    #Send cancel signal 
    if dispatcher != None:
        cancelled:bool = await dispatcher.cancel_job(job_id)

        if cancelled and db_worker!=None:
            db_job.status = "cancelled"
            await db_worker.update_job(db_job)
            return {"status": "cancelled", "job_id": job_id}
        else: 
            raise HTTPException(status_code=404, detail="Job not running or finished")
    

# Get active logs
@app.get("/jobs/{job_id}/logs")
async def get_logs(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job.logs:
        return {"logs": "Logs were not requested for this job."}

    # HARDCODED
    return {
        "job_id": job_id,
        "logs": [
            "Starting execution...",
            "Processing...",
            "Finished successfully."
        ]
    }

if __name__ == "__main__":
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)
