import os
import uuid
import httpx
from datetime import datetime, timezone
from typing import Optional, Annotated, Any, Dict, List
from fastapi import FastAPI, APIRouter, HTTPException, Depends, Header
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
INSTANT_APP_ID = os.environ.get('INSTANT_APP_ID')
INSTANT_ADMIN_TOKEN = os.environ.get('INSTANT_ADMIN_TOKEN')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', '').lower()
INSTANT_BASE = 'https://api.instantdb.com'

app = FastAPI()
api = APIRouter(prefix='/api')

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# Root path for Vercel
@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "Reward App API is LIVE on Vercel",
        "database": "connected" if INSTANT_APP_ID else "missing_keys"
    }

# --- InstantDB API Helpers ---
def _headers(as_token: Optional[str] = None):
    h = {"Authorization": f"Bearer {INSTANT_ADMIN_TOKEN}"}
    if as_token: h["X-Instant-As-Token"] = as_token
    return h

async def instant_query(q: Dict[str, Any], as_token: Optional[str] = None) -> Dict[str, Any]:
    if not INSTANT_APP_ID: return {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        url = f"{INSTANT_BASE}/admin/v1/apps/{INSTANT_APP_ID}/query"
        res = await client.post(url, json=q, headers=_headers(as_token))
        return res.json() if res.status_code == 200 else {}

async def instant_transact(steps: List[List[Any]]):
    if not INSTANT_APP_ID: return
    async with httpx.AsyncClient(timeout=10.0) as client:
        url = f"{INSTANT_BASE}/admin/v1/apps/{INSTANT_APP_ID}/transact"
        await client.post(url, json={'steps': steps}, headers=_headers())

# --- Routes ---
@api.post('/profile/bootstrap')
async def bootstrap(authorization: Annotated[Optional[str], Header()] = None):
    if not authorization: raise HTTPException(401, "No auth header")
    token = authorization.replace('Bearer ', '')
    u_data = await instant_query({'$users': {}}, as_token=token)
    user = u_data.get('$users', [])[0] if u_data.get('$users') else None
    if not user: raise HTTPException(401, "Invalid session")

    p_data = await instant_query({'profiles': {'$': {'where': {'user_id': user['id']}}}})
    if p_data.get('profiles'): return p_data['profiles'][0]

    pid = str(uuid.uuid4())
    profile = {
        'id': pid, 'user_id': user['id'], 'email': user['email'],
        'role': 'admin' if user['email'].lower() == ADMIN_EMAIL else 'user',
        'coins': 0, 'kyc_status': 'not_submitted', 'created_at': datetime.now(timezone.utc).isoformat()
    }
    await instant_transact([['update', 'profiles', pid, profile]])
    return profile

@api.get("/banners")
async def get_banners():
    res = await instant_query({'banners': {'$': {'where': {'is_active': True}}}})
    return res.get('banners', [])

app.include_router(api)
