from pydantic import BaseModel
from typing import List

class Group(BaseModel):
    group: str
    files: List[str]
    headers: List[str]

class TableData(BaseModel):
    groups: List[Group]

@router.post("/drop_and_create_table/")
async def drop_and_create_table(table_data: TableData, auth: bool = Depends(verify_auth)):
    """Receive JSON and log it without processing."""
    try:
        logger.info("🚀 Received request to drop and create tables.")
        logger.info(f"✅ Parsed JSON: {table_data.dict()}")
        return {"message": "JSON received and logged successfully"}
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
