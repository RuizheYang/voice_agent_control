"""
Compatibility layer for removed MongoDB documents.
All persistence has been migrated to MySQL tables in app.dbmodels.models.
"""
from app.dbmodels.models import ClueInfo as Clue, CallRecord as EndCallInfo

DOC_LIST = []
