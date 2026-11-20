from pydantic import BaseModel


class NtfyResult(BaseModel):
    id: str
    time: int
    expires: int
    event: str
    topic: str
    message: str
