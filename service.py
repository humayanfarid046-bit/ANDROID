import os
import uuid
import httpx
from datetime import datetime, timezone
from typing import Optional, Annotated, Any, Dict, List
from fastapi import FastAPI, APIRouter, HTTPException, Depends, Header
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()
api = APIRouter(prefix='/api')

# Load ENV
INSTANT_APP_ID = os.environ.get('INSTANT_APP_ID')
INSTANT_ADMIN_TOKEN = os.environ.get('INSTANT_ADMIN_TOKEN')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', '').lower()
INSTANT_BASE = 'https://api.instantdb.com'

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "Coin Earn API is Running",
        "db_connected": bool(INSTANT_APP_ID and INSTANT_ADMIN_TOKEN)
    }

# --- InstantDB API Helpers ---
async def instant_query(q: Dict[str, Any], as_token: Optional[str] = None) -> Dict[str, Any]:
    if not INSTANT_APP_ID: return {}
    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {INSTANT_ADMIN_TOKEN}"}
        if as_token: headers["X-Instant-As-Token"] = as_token
        url = f"{INSTANT_BASE}/admin/v1/apps/{INSTANT_APP_ID}/query"
        res = await client.post(url, json=q, headers=headers)
        return res.json() if res.status_code == 200 else {}

# --- Routes ---
@api.get("/banners")
async def get_banners():
    res = await instant_query({'banners': {'$': {'where': {'is_active': True}}}})
    return res.get('banners', [])

@api.get("/health")
async def health():
    return {"status": "ok"}

app.include_router(api)
