from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upload_handler import router as upload_router
from group_csvs import router as group_router
from drop_and_create_table import router as drop_create_router  # ✅ Import new route

app = FastAPI(title="FastAPI CSV Processing", description="Handles CSV uploads, grouping, and table creation.")

# ✅ Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Include API Routes
app.include_router(upload_router, prefix="/upload", tags=["Upload"])
app.include_router(group_router, prefix="/csv", tags=["CSV Processing"])
app.include_router(drop_create_router, prefix="/csv", tags=["Table Management"])  # ✅ Register new route
