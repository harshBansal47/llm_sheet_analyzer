


# 1. test importing google sheet
from app.services.sheets_service import SheetsService


def test_sheet_service():
    service_instane = SheetsService()
    dfs = service_instane.get_all_dataframes()
    print(dfs)

test_sheet_service()