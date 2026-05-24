import os
import uuid
import httpx
import random
import traceback
from datetime import datetime, timezone
from typing import Optional, Annotated, Any, Dict, List
from fastapi import FastAPI, HTTPException, Depends, Header
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()
INSTANT_APP_ID = os.environ.get('INSTANT_APP_ID')
INSTANT_ADMIN_TOKEN = os.environ.get('INSTANT_ADMIN_TOKEN')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', '').lower()
INSTANT_BASE = 'https://api.instantdb.com'

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# --- GLOBAL ERROR HANDLER ---
@app.middleware("http")
async def catch_exceptions_middleware(request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        return HTTPException(status_code=500, detail={
            "error": str(e),
            "trace": traceback.format_exc()
        })

# --- HELPERS ---
def _headers(as_token: Optional[str] = None):
    h = {"Authorization": f"Bearer {INSTANT_ADMIN_TOKEN}"}
    if as_token: h["X-Instant-As-Token"] = as_token
    return h

async def instant_query(q: Dict[str, Any], as_token: Optional[str] = None):
    async with httpx.AsyncClient(timeout=15.0) as client:
        url = f"{INSTANT_BASE}/admin/v1/apps/{INSTANT_APP_ID}/query"
        res = await client.post(url, json=q, headers=_headers(as_token))
        if res.status_code != 200:
            raise Exception(f"InstantDB Query Failed: {res.text}")
        return res.json()

async def instant_transact(steps: List[List[Any]]):
    async with httpx.AsyncClient(timeout=15.0) as client:
        url = f"{INSTANT_BASE}/admin/v1/apps/{INSTANT_APP_ID}/transact"
        res = await client.post(url, json={'steps': steps}, headers=_headers())
        if res.status_code != 200:
            raise Exception(f"InstantDB Transact Failed: {res.text}")

# --- ROUTES ---
@app.get("/")
async def root():
    return {"status": "ok", "config": {"app_id": bool(INSTANT_APP_ID), "token": bool(INSTANT_ADMIN_TOKEN)}}

@app.post('/api/profile/bootstrap')
async def bootstrap(authorization: Annotated[Optional[str], Header()] = None):
    if not authorization: raise HTTPException(401, "Missing Auth Header")
    token = authorization.replace('Bearer ', '')

    # 1. Get User
    u_data = await instant_query({'$users': {}}, as_token=token)
    users = u_data.get('$users', [])
    if not users: raise HTTPException(401, "Session Expired. Please Logout and Login again.")
    user = users[0]

    # 2. Get/Create Profile
    p_data = await instant_query({'profiles': {'$': {'where': {'user_id': user['id']}}}})
    profs = p_data.get('profiles', [])

    if profs:
        return profs[0]

    # Create new
    pid = str(uuid.uuid4())
    profile = {
        'id': pid, 'user_id': user['id'], 'email': user['email'],
        'role': 'admin' if user['email'].lower() == ADMIN_EMAIL else 'user',
        'coins': 0, 'kyc_status': 'not_submitted',
        'created_at': datetime.now(timezone.utc).isoformat()
    }
    await instant_transact([['update', 'profiles', pid, profile]])
    return profile

@app.get('/api/banners')
async def get_banners():
    res = await instant_query({'banners': {'$': {'where': {'is_active': True}}}})
    return res.get('banners', [])
