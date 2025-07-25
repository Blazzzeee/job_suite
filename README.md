The first part would be a webserver which listens to incoming job requests \n
**-- desc server**
The server is a FastAPI server that listens onto oncoming job requests. The job requests are sent via a CLI which
can include various information like priority, timeouts, retries, display logs, (more to add).

The server uses some sort of parsing mechanism to understand JSON (the CLI packs all this data inside JSON).
**TODO:** The parser, after decoding the request, checks the validity of the request. The validity is in form of type of job as well as the auth mechanism.

---

**-- desc scheduler**
The server dispatches the job to the scheduler to take care of. The scheduler runs on a separate thread or program ensuring no jobs shall be missed while the scheduler may be busy in other important tasks.
After receiving this info using IPC, it emulates a job queue. The job queue consists of a retry queue, holding state information on already enqueued processes, and a priority-level compatible incoming queue.

---

**-- desc remote instance**
The remote instance maintains a TCP socket and some sort of exclusive buffer where the scheduler and instance communicate. The reason for sockets is performance gains. The remote instance has a buffer read-in event when a new job is received. The instance starts a new thread and allows execution of the received job.

Additionally, if there was a log request by the CLI, then the instance forms a remote socket similar to a pipe to display logs and provide it to the client.

---

**-- Misc**
The user can request the state of a job. This first contacts the scheduler which shall maintain the information of scheduled jobs, and construct the first-degree information from it.
Additionally, the TCP buffer between the scheduler is filled with a read-in event requesting the current state of that thread. The scheduler provides job state by a combination of both these info sources.
The server shall wait for a buffer write on buffer for a fixed time.

All the data is updated in an SQLite DB for result, metrics.

---

## TODO

1. **FastAPI Server:** Implement endpoints to accept job submissions and job status queries.
2. **Job Scheduler:** Build a queue management system with retry logic and priority-based job handling.
3. **TCP Socket for Remote Execution:** Set up TCP communication for job dispatching and logging.
4. **Job State Persistence:** Set up SQLite to track job state, results, and metrics.
5. **CLI Tool:** Create a CLI tool to interact with the system.
6. **Testing:** Write unit tests for edge cases, race conditions, and database consistency.

---

# FastAPI Server

### Endpoints:

| Endpoint          | Method | Purpose               |
| ----------------- | ------ | --------------------- |
| `/jobs`           | POST   | Submit a new job      |
| `/jobs/{job_id}`  | GET    | Check status of a job |
| `/jobs`           | GET    | List all jobs         |
| `/jobs/{job_id}`  | DELETE | Cancel a job          |
| `ws/jobs/{id}/logs` | GET    | Fetch job logs        |

**-- Status: done**

---

# Updates

Initial approach was bypassing the GIL, since we don’t want to process the Server and scheduler on the same thread. However, bypassing GIL enforces thread safety as my responsibility.

Usage of a mutex to ensure that there are no race conditions within shared buffer, but this enforces a condition of say the program is currently on the scheduler thread, meanwhile a new HTTP POST comes in — how do we deal with this?
After tons of research, the Python GIL is not our enemy. After documentation dive, my solution is based upon these conclusions:
The GIL ensures that there are no race conditions from the user running multiple threads; it ensures Python's internal safety.
We can depend upon async events such as buffer reads and writes and mutex locks for shared resources only. GIL protects the interpreter from race conditions, while the user (we) can make smart design choices i.e., appropriate async handlers.

---

## Detailed Description

**The answer is:** No, the request will not be lost.

The web server will still accept the connection, even if the scheduler thread is holding the Global Interpreter Lock (GIL).

### Why the Request Isn't Lost

The GIL doesn't block everything. It specifically prevents two Python threads from executing Python bytecode at the exact same time.

#### What actually happens:

* **Network I/O is Not Python Code:** An incoming network request is first handled by our computer's operating system (OS). The OS accepts the TCP connection and places the incoming data into a network buffer. This happens completely outside of Python and the GIL.

* **I/O Operations Release the GIL:** The FastAPI server (running on Uvicorn) is highly optimized for I/O-bound tasks. When it's waiting for a new request, it's essentially telling the OS, "Wake me up when something happens on this network socket." During this waiting period, the server thread releases the GIL, allowing our scheduler thread to run.

* **Python's Thread Switching:** Even if our scheduler is in the middle of a heavy computation (a CPU-bound task), the Python interpreter automatically switches which thread holds the GIL every few milliseconds. This ensures that the server thread will get a chance to run, check the OS buffer for the new request, and begin processing it.

So, while the GIL means we don't get true parallelism for CPU-bound tasks, it's designed to be very efficient for I/O-bound tasks like running a web server.
**The request will be safely waiting in the OS network buffer until the server thread gets the GIL and processes it.**

---

# Scheduler
Implemeted via JobQueue
Note this is only an in memory represntation and does not provide data to user in any form , it is only used for faster access to Scheduler API

The task scheduler is responsible for every operation that involves any form of queue interaction , such as incoming job , job dispatch etc

The task scheduler should expose an API to enqueue and dequeue jobs as a Priority Queue as well as perform this in an async manner to not block the Server , and other parts .
The async behaviour of scheduler adds a race condition , consider a case when a user does a POST , our post {job_id} controller should be
responsible for the enqueue , and call an async coroutine enqueue_job , but before that operation could finish another reequest arrives 
assuming the CPU was doing a expensive operation like a memory lookup like PriorityQueue field , then it is almost certain that the contoller would get CPU access , then if the another POST by user leads to a similar enqueue and then it again goes into poll for memory lookup and 
our previous enqueue takes place again , and completes , then the 2nd request completes the first request buffer write is lost

## solution
Usage of a mutex_lock , the queue mutex shall be locked via each enqueue and dequeue to ensure that these operations are atomic , altho 
another request can break atomicity for this operation , but this request if it causes a race condition would certainly use our JobQueue api
then this mutex shall lock the other request operation to safeguard our JobQueue


# Database Worker

Consider the case where the dispatcher after dispatching job interrupts , or the server or the JobQueue crashes due to some unhandled 
exception , but since up till now we are only storing jobs in memory this introduces need for data persistance , then some proposed solutions to handle this are

## Solution 1
Abandon usage of JobQueue , and use database for all operations , this unifies the data logic as being completely dependent on datbase 
however , we are not discussing how the db will be Implemeted in this solution , but every operation require interaction of the DBWorker and ffile write operations for DB require DB State synchronization along with added kernel intervention for reading and writing onto DB file , this will completely slow up the queue api which should be exposed as a Scheduler 
## Solution 2
Usage of a database as a backup for addtional information of JobQueue member Job ,this is initally thought of as a routine that after some 
DATBASE_DELAY writes the new state of our queue to database , then we must use this database for the sake of backup only as it becomes unusable for sharing information among other threads , routines and co-routines , since the data is currently unrelibale due to the fact any change 
could have occurred in that delay duration . Like serving the user a GET request for /jobs/{job_id} , then we dont know for sure if that job has changed its state or not. , but this solves the problem of data persistance
## Solution 3
Usage of database for IPC , as mentioned we need some sort of socket to communicate to the remote instance , then the database can be hosted
remotly , and the Scheduler , JobQueue , Dispatcher and remote instance program all would need to contact the DB for operations , this introduces a significant delay for the these operations compared to something local , even tho some of it can be avoided using appropriate async 
handling , but the Client side would need some mechanism to notified to fetch from db , this makes the design more complex and well be as fastas the datbase is , which is always slower than local storage , but this will solve a lot of issues
## Solution 4 
Every updation to JobQueue is also updated in the database , the Job class and database model Job should be completely compatible and we shall use sqlite with an async engine to ensure that none of these operations block the server , since we are not making seperate threads for eacg component , then this database should expose an async API for db operations to the model model.Job and since the order of these operations is chosen to be async , some form of synchronization mechanism is required . 

Sync mechanism -> usage of a mutex lock that serves as access to physical database file reads and writes , additonaly we must ensure the atomicity for these operations to prevent concurrent , out of order writes , etc

## Solution used (Solution 4)-> AyncDBWorker class (present in db.py)

# RemoteInstances
 -- Remaining


# CLI usage 
Usage: cli.py [OPTIONS] COMMAND [ARGS]...
### Options
Usage: cli.py [COMMAND] -- help 
(Command specfic)
### Commands
submit                 
    Submit a new job to the scheduler.

status                 
    Get the status of a specific job.

list                   
    List all jobs.

cancel                 
    Cancel a job by ID.

logs                   
    Stream real-time logs for a specific job using WebSocket.
Note: use config.toml for defaults

## Requirements

.env file with remote instances REMOTE_INSTANCES=[{"host": "localhost", "user": "", "password": "", "port": }]

