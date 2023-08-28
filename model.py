from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, Field, validator

from teems.nordpoolservice.exceptions import NordPoolException
from teems.signal.model import PriceUnits

from common.enums.price_area import PriceArea
from common.schemas.price.currency import PriceCurrency


class NordPoolStatus(str, Enum):
    provisional = "P"
    official = "O"


class NordpoolPriceAttribute(BaseModel):
    name: str
    role: Optional[str]
    value: str


@dataclass
class DummyNordpoolPriceValue:
    start_time: datetime
    end_time: datetime
    value: Any


class NordpoolPriceValue(BaseModel):
    start_time: datetime = Field(alias="startTime")
    end_time: datetime = Field(alias="endTime")
    value: float

    @validator("start_time")
    def _remove_tz_start_time(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=None)

    @validator("end_time")
    def _remove_tz_end_time(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=None)


attr_type = TypeVar("attr_type", bound=Enum)


class NordPoolTokenResponse(BaseModel):
    access_token: str
    expires_at: datetime
    token_type: str


class NordPoolApiTokenResponse(BaseModel):
    expires_in: int
    access_token: str
    token_type: str


@dataclass
class Interval:
    start: datetime
    end: datetime
