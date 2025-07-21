import typer
import httpx
import toml
import asyncio
from pathlib import Path
import websockets

app = typer.Typer()

CONFIG_PATH = Path("config.toml")
API_URL = "http://localhost:8000"
API_WS_URL = "ws://localhost:8000/ws/jobs"

# Load defaults from config.toml
DEFAULTS = {}
if CONFIG_PATH.exists():
    DEFAULTS = toml.load(CONFIG_PATH)
else:
    typer.secho("[!] config.toml not found. Defaults will not be loaded.", fg=typer.colors.YELLOW)

@app.command()
def submit(
    command: str = typer.Argument(..., help="Shell command to run remotely"),
    name: str = typer.Option(None, help="Job name"),
    priority: str = typer.Option(None, help="Job priority (low, mid, high)"),
    timeout: int = typer.Option(None, help="Timeout in seconds"),
    retries: int = typer.Option(None, help="Number of retries"),
    logs: bool = typer.Option(None, help="Enable live log streaming"),
    params: str = typer.Option(None, help="Additional job parameters")
):
    """
    Submit a new job to the scheduler.
    """
    payload = {
        "name": name or DEFAULTS.get("name", "default-job"),
        "command": command,
        "params": params or DEFAULTS.get("params", ""),
        "priority": priority or str(DEFAULTS.get("priority", "mid")),
        "timeout": timeout if timeout is not None else DEFAULTS.get("timeout", 60),
        "retries": retries if retries is not None else DEFAULTS.get("retries", 0),
        "logs": logs if logs is not None else DEFAULTS.get("logs", True),
    }

    async def do_submit():
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{API_URL}/jobs/", json=payload)
            try:
                typer.echo(response.json())
            except Exception:
                typer.echo(response.text)

    asyncio.run(do_submit())

@app.command()
def status(job_id: str):
    """
    Get the status of a specific job.
    """
    async def do_status():
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_URL}/jobs/{job_id}")
            if response.status_code == 200:
                typer.echo(response.json())
            else:
                typer.secho(f"[!] Error {response.status_code}: {response.text}", fg=typer.colors.RED)

    asyncio.run(do_status())

@app.command("list")
def list_jobs():
    """
    List all jobs.
    """
    async def do_list():
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_URL}/jobs/")
            typer.echo(response.json())

    asyncio.run(do_list())

@app.command()
def cancel(job_id: str):
    """
    Cancel a job by ID.
    """
    async def do_cancel():
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{API_URL}/jobs/{job_id}")
            typer.echo(response.json())

    asyncio.run(do_cancel())

@app.command()
def logs(job_id: str):
    """
    Stream real-time logs for a specific job using WebSocket.
    """
    async def stream_logs():
        uri = f"{API_WS_URL}/{job_id}/logs"
        try:
            async with websockets.connect(uri) as websocket:
                typer.echo(f"Connected to job {job_id}. Streaming logs...\n")
                while True:
                    try:
                        log_line = await websocket.recv()
                        typer.echo(log_line)
                    except websockets.exceptions.ConnectionClosedOK:
                        typer.echo("\n[WebSocket closed gracefully]")
                        break
                    except websockets.exceptions.ConnectionClosedError as e:
                        typer.echo(f"\n[WebSocket error: {e}]")
                        break
        except Exception as e:
            typer.echo(f"Error connecting to WebSocket: {e}")

    asyncio.run(stream_logs())

if __name__ == "__main__":
    app()
