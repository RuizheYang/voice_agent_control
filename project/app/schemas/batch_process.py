from typing import List, Dict, Any
from pydantic import BaseModel

class ClueDetailItem(BaseModel):
    clueId: str
    detail: Dict[str, Any]

class BatchInitRequest(BaseModel):
    code: int = 0
    message: str = "OK"
    total: int = 0
    data: List[ClueDetailItem]
    
class BatchCancelRequest(BaseModel):
    clueIds: List[str]

class BatchResetRequest(BaseModel):
    clueIds: List[str]
    state: str = "rejected"

