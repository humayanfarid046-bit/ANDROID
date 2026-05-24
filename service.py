import os
import uuid
import httpx
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional, Annotated, Any, Dict, List
from fastapi import FastAPI, APIRouter, HTTPException, Depends, Header
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# 1. Setup & Environment
load_dotenv()
INSTANT_APP_ID = os.environ.get('INSTANT_APP_ID')
INSTANT_ADMIN_TOKEN = os.environ.get('INSTANT_ADMIN_TOKEN')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', '').lower()
INSTANT_BASE = 'https://api.instantdb.com'

# 2. Main App Instance
app = FastAPI()
api = APIRouter(prefix='/api')

# 3. CORS
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# --- DB HELPERS ---
def _headers(as_token: Optional[str] = None):
    h = {"Authorization": f"Bearer {INSTANT_ADMIN_TOKEN}"}
    if as_token: h["X-Instant-As-Token"] = as_token
    return h

async def instant_query(q: Dict[str, Any], as_token: Optional[str] = None):
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

# --- CORE LOGIC ---
def now_iso(): return datetime.now(timezone.utc).isoformat()
def public_profile(p):
    return {
        'id': p['id'], 'user_id': p['user_id'], 'email': p['email'],
        'role': p.get('role', 'user'), 'coins': int(p.get('coins') or 0),
        'kyc_status': p.get('kyc_status', 'not_submitted'),
        'referral_code': p.get('referral_code', '')
    }

async def current_profile(authorization: Annotated[Optional[str], Header()] = None):
    if not authorization: raise HTTPException(401, "Missing Auth")
    token = authorization.replace('Bearer ', '')
    u_data = await instant_query({'$users': {}}, as_token=token)
    user = u_data.get('$users', [])[0] if u_data.get('$users') else None
    if not user: raise HTTPException(401, "Invalid Session")

    p_data = await instant_query({'profiles': {'$': {'where': {'user_id': user['id']}}}})
    profs = p_data.get('profiles', [])
    if profs: return profs[0]

    pid = str(uuid.uuid4())
    profile = {
        'id': pid, 'user_id': user['id'], 'email': user['email'],
        'role': 'admin' if user['email'].lower() == ADMIN_EMAIL else 'user',
        'coins': 0, 'kyc_status': 'not_submitted', 'created_at': now_iso()
    }
    await instant_transact([['update', 'profiles', pid, profile]])
    return profile

# --- ROUTES ---
@app.get("/")
async def root():
    return {"status": "ok", "message": "Reward App Backend is LIVE", "db": bool(INSTANT_APP_ID)}

@api.post('/profile/bootstrap')
async def bootstrap(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    return public_profile(profile)

@api.get('/banners')
async def get_banners():
    res = await instant_query({'banners': {'$': {'where': {'is_active': True}}}})
    return res.get('banners', [])

@api.get('/admin/stats')
async def admin_stats(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    if profile.get('role') != 'admin': raise HTTPException(403)
    all_p = await instant_query({'profiles': {}})
    profs = all_p.get('profiles', [])
    return {
        'total_users': len(profs),
        'total_coins_circulating': sum(int(p.get('coins') or 0) for p in profs),
        'pending_kyc': sum(1 for p in profs if p.get('kyc_status') == 'pending')
    }

app.include_router(api)
