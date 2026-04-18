

import asyncio
import json

from app.config import Settings
from app.models.models import IncomingMessage
from app.services.query_orchestrator import get_orchestrator


import gspread
from google.oauth2.service_account import Credentials

from app.services.sheets_service import CredentialsError


from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import json


creds_json = Settings().google_credentials_json
if not creds_json:
    raise CredentialsError("google_credentials_json is empty or not set.")
creds_dict = json.loads(creds_json)
creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

print(result)


# async def test_llm_service():
#     msg = IncomingMessage(
#             platform="telegram",
#             user_id="testuser",
#             text = "I want to know about person who is avaibale in phase 1 and have no court case and recieved greater than 50%  and type is new"
#             # text="I want to know about customers who are having annual income of greater that 100000 and spending score greater than 75 percent and online purchase greater than 100",
#         )
#     orchestrator = get_orchestrator()
#     response_text = await orchestrator.handle(msg)
#     print(response_text)

# asyncio.run(test_llm_service())