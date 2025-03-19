from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upload_handler import router as upload_router
from group_csvs import router as group_router
from drop_and_create_table import router as drop_create_router
from load_data import router as load_data_router  # ✅ Added New Router

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
app.include_router(drop_create_router, prefix="/csv", tags=["Table Management"])
app.include_router(load_data_router, prefix="/csv", tags=["Data Loading"])  # ✅ Added Load Data API
