from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import uuid
import uvicorn
import utils 
import models
import db
import asyncio

db_worker = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_worker

    # STARTUP
    session = await utils.init_db()
    db_worker = db.AsyncDBWorker(session=session)
    worker_task = asyncio.create_task(db_worker.run())
    print("[INFO] Database worker started")

    yield 

    # SHUTDOWN
    if db_worker:
        await db_worker.stop()
        print("[INFO] Database worker stopped")
    worker_task.cancel()


app = FastAPI(lifespan=lifespan)


MAX_RETRIES = 10

queue = utils.Queue()
queue_mutex = asyncio.Lock()

# # TEMP: Simulate db  REPLACED
jobs = {}

# Submit a job for execution
@app.post("/jobs/", status_code=201)
async def post_job(job: utils.JobRequest):
    job_id = str(uuid.uuid4())
    job_instance = utils.Job(job_id, job)

    try: 
        db_job_instance = models.Job(
                name=job.name,
                command=job.command,
                params=job.params,
                priority=job.priority,
                timeout=job.timeout,
                status="queued",
            )
    except Exception as e:
        print(f"Recived invalid request {e}")
        raise HTTPException(status_code=400, detail="Invalid argument (invalid request payload)") 

    try:
        await queue.enqueue_job(job_instance, job_instance.priority,queue_mutex) 
        await db_worker.add_job(db_job_instance)
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
            "job_id": job.id,
            "command": job.command,
            "params": job.params,
            "priority": job.priority,
            "timeout": job.timeout,
            "status": job.status,
            "result": job.result
        }

# Get list of all running jobs
@app.get("/jobs/")
async def list_jobs():
    return {
        "jobs": [
            {
                "job_id": j.job_id,
                "command": j.command,
                "priority": j.priority,
                "status": j.status
            }
            for j in jobs.values()
        ]
    }

# Cancel a running job
@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status in ["completed", "failed"]:
        return {"status": "cannot cancel", "reason": f"job already {job.status}"}

    job.status = "cancelled"
    return {"status": "cancelled", "job_id": job_id}

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
