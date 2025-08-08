from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime


class Job(SQLModel, table=True):
    job_id: str = Field(index=True, unique=True)
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    command: str
    params: Optional[str] = None
    priority: str = Field(default="mid")
    timeout: Optional[int] = None
    status: str = Field(default="queued")
    # "queued", "running", "completed", "failed", "cancelled"
    cancel_requested: bool = Field(default=False)
    logs: Optional[str] = None
    result: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    updated_at: datetime = Field(default_factory=datetime.now)
    cancellable:bool = False

    def __repr__(self):
        return f"<Job id={self.id} name={self.name} status={self.status} priority={self.priority}>"
