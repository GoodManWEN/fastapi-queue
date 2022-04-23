import os , sys
sys.path.append(os.getcwd())
import pytest
from fastapi_queue import *

@pytest.mark.asyncio
async def test_import():
    ...