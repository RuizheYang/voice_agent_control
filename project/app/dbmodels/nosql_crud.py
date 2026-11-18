"""
Compatibility layer for removed MongoDB CRUD.
All CRUD functions now execute on MySQL tables via app.dbmodels.crud.
"""
from app.dbmodels.crud import (
    create_clue,
    get_clue_intake_time,
    get_clue_meta,
    create_call_end_record,
    update_clue_next_call_work_phone,
    prepare_call_params,
    update_end_call_info_outbound_report,
)
from app.dbmodels.models import ClueInfo, CallRecord

async def create_end_call_info(end_call_request):
    return await create_call_end_record(end_call_request)

async def get_raw_clue_from_mongo(clueId: str):
    clue = await ClueInfo.filter(clue_id=clueId).first()
    return dict(clue) if clue else None

async def list_end_call_infos_from_mongo(clueId: str):
    calls = await CallRecord.filter(clue_id=clueId).order_by("-create_time").all()
    return [dict(c) for c in calls]