from fastapi import FastAPI, HTTPException
import uuid
import uvicorn
import utils 
import models
import db
import asyncio

app = FastAPI()

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

    db_job_instance = models.Job(
            name=job.name,
            command=job.command,
            params=job.params,
            priority=job.priority,
            timeout=job.timeout,
            status="queued",
        )

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
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job.job_id,
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


# Enqueue a job into the priority queue
if __name__ == "__main__":
    session = utils.init_db()
    db_worker = db.AsyncDBWorker(session=session)
    asyncio.create_task(db_worker.run())
    print(f"[INFO] Initialised Database worker thread for persistent storage")
    
    print("[INFO] Starting FastAPI app inside a thread...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
