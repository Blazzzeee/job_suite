import typer
import httpx
import toml
import asyncio
from pathlib import Path

app = typer.Typer()
CONFIG_PATH = Path("config.toml")
API_URL = "http://localhost:8000"
DEFAULTS = {}

# Load defaults from config.toml if available
if CONFIG_PATH.exists():
    DEFAULTS = toml.load(CONFIG_PATH)
else:
    typer.secho("[!] config.toml not found. Defaults will not be loaded.", fg=typer.colors.YELLOW)


@app.command()
def submit(command: str):
    """
    Submit a new job with optional defaults from config.toml.
    """
    payload = {
        "name": DEFAULTS.get("name", "default-job"),
        "command": command,
        "params": DEFAULTS.get("params", ""),
        "priority": str(DEFAULTS.get("priority", "mid")),  # must be string like "high"
        "timeout": DEFAULTS.get("timeout", 60),
        "retries": DEFAULTS.get("retries", 0),
        "logs": DEFAULTS.get("logs", True),
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
    Get logs for a specific job.
    """
    async def do_logs():
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_URL}/jobs/{job_id}/logs")
            typer.echo(response.json())

    asyncio.run(do_logs())


if __name__ == "__main__":
    app()
