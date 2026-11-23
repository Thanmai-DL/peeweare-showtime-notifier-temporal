from datetime import datetime, timedelta
from enum import Enum
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.alias_generators import to_snake


class Tags(Enum):
    users = "users"
    movies = "movies"
    monitoring = "monitoring"
    lists = "lists"


class ShowingType(str, Enum):
    nowshowing = "nowshowing"
    comingsoon = "comingsoon"


class MonitoringRequest(BaseModel):
    movie_id: str
    name: str | None = None
    theater_id: str
    release_date: datetime = Field(
        default_factory=lambda: datetime.now(tz=ZoneInfo("Asia/Kolkata"))
    )
    expiry_date: datetime | None = None
    imax: bool = False
    schedule_id: str | None = None
    showtimes: dict | None = Field(default_factory=dict)
    updates: int = 5
    model_config = ConfigDict(alias_generator=to_snake, extra="forbid")

    @model_validator(mode="after")
    def populate_derived_fields(self) -> "MonitoringRequest":
        if self.expiry_date is None:
            self.expiry_date = self.release_date + timedelta(days=1)

        if self.schedule_id is None:
            self.schedule_id = f"start-monitoring-schedule-{self.movie_id}-{self.theater_id}-{int(self.release_date.timestamp())}"

        return self


class MonitoringRequestWithId(MonitoringRequest):
    document_id: str | None = None


class NotificationPayload(BaseModel):
    payload: dict[str, str]
