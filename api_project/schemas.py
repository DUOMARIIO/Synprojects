from pydantic import BaseModel, Field
from typing import Optional


class CallTranscribation(BaseModel):
    start_second: float = Field(example=0.05)
    end_second: float = Field(example=5.6)
    user_name: Optional[str] = Field(example='Dubai Denis')
    transcribation: str = Field(example='Hello World!')

    class Config:
        orm_mode = True
