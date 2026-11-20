from datetime import datetime

from pydantic import BaseModel


class MonitoringInput(BaseModel):
    movie_id: str
    name: str
    theater_id: str
    release_date: datetime
    expiry_date: datetime
    imax: bool
    schedule_id: str
    showtimes: dict
    updates: int


class MonitoringInputWithId(MonitoringInput):
    document_id: str
