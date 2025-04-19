from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upload_handler import router as upload_router
from group_csvs import router as group_router
from create_and_load_tables import router as combined_router
#import asyncio
#import json

app = FastAPI(title="FastAPI CSV Processing", description="Handles CSV uploads, grouping, and analysis.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload_router, prefix="/upload", tags=["Upload"])
app.include_router(group_router, prefix="/csv", tags=["CSV Processing"])
app.include_router(combined_router, prefix="/csv", tags=["Create and Load Tables"])
