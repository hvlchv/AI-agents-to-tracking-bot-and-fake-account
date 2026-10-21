# TODO: Implement EventV1, FeatureV1, and EvidenceV1 with validation.
from decimal import Decimal
from datetime import datetime, timezone
from typing import Literal
from pydantic import (
    AwareDatetime, 
    BaseModel, 
    Field,
    ConfigDict,
    field_validator,
    model_validator
)
import json

Source = Literal["rees46_multi", "synthetic", "demo_app"]
EventType = Literal["view", "cart", "remove_from_cart", "purchase", "register", "login", "redeem"]

class EventV1(BaseModel):
    model_config = ConfigDict(extra = "forbid", str_strip_whitespace = True)
    event_id: str = Field(min_length = 1, max_length = 256)
    user_id: str = Field(min_length = 1, max_length = 256)
    dataset_version: str = Field(min_length = 1, max_length = 256)
    source: Source
    event_time: AwareDatetime
    event_type: EventType
    
    user_session: str = Field(default = None, min_length = 1, max_length = 256)
    product_id: str = Field(default = None, min_length = 1, max_length = 256)
    category_id: str = Field(default = None, min_length = 1, max_length = 256)
    category_code: str = Field(default = None, min_length = 1, max_length = 256)
    brand: str = Field(default = None, min_length = 1, max_length = 256)
    price: Decimal | None = None
    ip_token: str | None = None
    currency: str | None = None
    device_id: str | None = None
    coupon_id: str | None = None
    attributes: dict = Field(default_factory = dict)
    
    @field_validator("event_time")
    @classmethod
    def convert_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(timezone.utc)
    
    @field_validator("price")
    @classmethod
    def check_price(cls, value: Decimal | None) -> Decimal:
        if value is not None and not value.is_finite():
            raise("Value la gia tri huu han")
        return value
    
class FeatureV1(BaseModel):
    model_config = ConfigDict(extra = "forbid")
    
    
class EvidenceV1(BaseModel):
    model_config = ConfigDict(extra = "forbid")
    