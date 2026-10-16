"""Strict routing-agent contract. Never interpret requested_at as the server clock."""

from datetime import datetime
from typing import Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from donor_targeting.config import EMBEDDING_DIM, PROGRAM_AREAS


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Content(StrictModel):
    content_id: int = Field(gt=0)
    subject: str = Field(min_length=1, max_length=1000)
    program_area: str
    dense_embedding: list[float] = Field(min_length=EMBEDDING_DIM, max_length=EMBEDDING_DIM)

    @field_validator("program_area")
    @classmethod
    def known_area(cls, value):
        if value not in PROGRAM_AREAS:
            raise ValueError("unknown program area")
        return value

    @field_validator("dense_embedding")
    @classmethod
    def valid_embedding(cls, value):
        norm = np.linalg.norm(value)
        if not np.isfinite(norm) or not 0.9 <= norm <= 1.1:
            raise ValueError("embedding must be finite and approximately unit length")
        return value


class Audience(StrictModel):
    type: Literal["all_active_subscribers"]


class Destination(StrictModel):
    crm: Literal["mock"]
    list_name: str = Field(min_length=1, max_length=200)


class TargetRequest(StrictModel):
    request_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_.:-]+$")
    org_id: Literal["demo-charity"]
    requested_at: datetime
    content: Content
    audience: Audience
    destination: Destination

    @field_validator("requested_at")
    @classmethod
    def aware_timestamp(cls, value):
        if value.tzinfo is None:
            raise ValueError("requested_at must include a timezone")
        return value
