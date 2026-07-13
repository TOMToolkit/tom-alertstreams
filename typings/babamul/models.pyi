"""Type stub for babamul.models — covers only what tom_alertstreams uses."""

from datetime import datetime

from pydantic import BaseModel

class ZtfCandidate(BaseModel):
    jd: float
    ra: float
    dec: float
    magpsf: float
    sigmapsf: float
    @property
    def datetime(self) -> datetime: ...

class LsstCandidate(BaseModel):
    ra: float
    dec: float
    magpsf: float
    sigmapsf: float
    jd: float
    objectId: str
    @property
    def datetime(self) -> datetime: ...

class ZtfAlert(BaseModel):
    candid: int
    objectId: str
    candidate: ZtfCandidate
    topic: str | None

class LsstAlert(BaseModel):
    candid: int
    objectId: str
    candidate: LsstCandidate
    topic: str | None
