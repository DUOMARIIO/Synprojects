from fastapi import FastAPI, APIRouter
from config import settings
from api_router import api_router

app = FastAPI(title=settings.PROJECT_NAME,
              docs_url=f'{settings.BASE_ROUTE}/docs',
              redoc_url=f'{settings.BASE_ROUTE}/redoc',
              openapi_url=f'{settings.BASE_ROUTE}/openapi.json')
router = APIRouter()

app.include_router(api_router, prefix=settings.BASE_ROUTE)
