from sqlalchemy import Column, Integer, String, Float

from db import Base


class CallTranscribation(Base):
    source_system = Column(String, primary_key=True)
    audio_id = Column(Integer, primary_key=True)
    start_second = Column(Float, primary_key=True)
    end_second = Column(Float)
    user_name = Column(String, primary_key=True)
    transcribation = Column(String)
