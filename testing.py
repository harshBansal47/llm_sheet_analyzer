

import asyncio
import json

from app.config import Settings
from app.models.models import IncomingMessage
from app.services.query_orchestrator import get_orchestrator



async def test_llm_service():
    msg = IncomingMessage(
            platform="telegram",
            user_id="testuser",
            text = "Find a person named Gaurav khana"
            # text="I want to know about customers who are having annual income of greater that 100000 and spending score greater than 75 percent and online purchase greater than 100",
        )
    orchestrator = get_orchestrator()    
    response_text = await orchestrator.handle(msg)
    print(response_text)

asyncio.run(test_llm_service())





