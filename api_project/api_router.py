from typing import Generator, Any, List

from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException

from crud import calltranscribation
import schemas
from db import SessionLocal


def get_db() -> Generator:
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()


api_router = APIRouter()


@api_router.get("/calls/{source_system}/{audio_id}",
                response_model=List[schemas.CallTranscribation])
def read_call_transcribation(*,
                             db: Session = Depends(get_db),
                             audio_id: int,
                             source_system: str) -> Any:
    """
    Получить транскрибацию звонка по id
    """
    transcribation = calltranscribation.get_multi_by_audio_id(
        db=db, audio_id=audio_id, source_system=source_system)
    if not audio_id:
        raise HTTPException(status_code=404, detail="Транскрибация не найдена")
    if source_system != 'oktell':
        raise HTTPException(status_code=405,
                            detail='Указанный source_system не поддерживается')
    return transcribation
