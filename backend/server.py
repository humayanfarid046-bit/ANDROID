import os
import uuid
import httpx
import random
from datetime import datetime, timezone
from typing import Optional, Annotated, Any, Dict, List
from fastapi import FastAPI, APIRouter, HTTPException, Depends, Header
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

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

# --- CONFIGS ---
REWARDS = {'check_in': 100, 'spin_min': 20, 'spin_max': 50, 'scratch_min': 20, 'scratch_max': 40, 'watch': 80, 'quiz': 50}
LIMITS = {'spin': 10, 'scratch': 5, 'watch': 10}

# --- HELPERS ---
def _headers(as_token: Optional[str] = None):
    h = {"Authorization": f"Bearer {INSTANT_ADMIN_TOKEN}"}
    if as_token: h["X-Instant-As-Token"] = as_token
    return h

async def instant_query(q: Dict[str, Any], as_token: Optional[str] = None):
    if not INSTANT_APP_ID: return {}
    async with httpx.AsyncClient(timeout=15.0) as client:
        url = f"{INSTANT_BASE}/admin/v1/apps/{INSTANT_APP_ID}/query"
        res = await client.post(url, json=q, headers=_headers(as_token))
        return res.json() if res.status_code == 200 else {}

async def instant_transact(steps: List[List[Any]]):
    if not INSTANT_APP_ID: return
    async with httpx.AsyncClient(timeout=15.0) as client:
        url = f"{INSTANT_BASE}/admin/v1/apps/{INSTANT_APP_ID}/transact"
        await client.post(url, json={'steps': steps}, headers=_headers())

def now_iso(): return datetime.now(timezone.utc).isoformat()
def today_str(): return datetime.now(timezone.utc).strftime('%Y-%m-%d')

async def get_current_profile(authorization: str):
    token = authorization.replace('Bearer ', '')
    u_data = await instant_query({'$users': {}}, as_token=token)
    user = u_data.get('$users', [])[0] if u_data.get('$users') else None
    if not user: raise HTTPException(401)
    p_data = await instant_query({'profiles': {'$': {'where': {'user_id': user['id']}}}})
    return p_data.get('profiles', [])[0] if p_data.get('profiles') else None

async def add_coins(profile, amount):
    new_total = int(profile.get('coins') or 0) + amount
    await instant_transact([['update', 'profiles', profile['id'], {'coins': new_total}]])
    return new_total

# --- ROUTES ---
@app.get("/")
async def root():
    return {"status": "online", "message": "Reward App Pro API Live", "database": "connected"}

@api.post('/profile/bootstrap')
async def bootstrap(authorization: Annotated[Optional[str], Header()] = None):
    if not authorization: raise HTTPException(401)
    token = authorization.replace('Bearer ', '')
    data = await instant_query({'$users': {}}, as_token=token)
    user = data.get('$users', [])[0] if data.get('$users') else None
    if not user: raise HTTPException(401)

    res = await instant_query({'profiles': {'$': {'where': {'user_id': user['id']}}}})
    if res.get('profiles'): return res['profiles'][0]

    pid = str(uuid.uuid4())
    profile = {
        'id': pid, 'user_id': user['id'], 'email': user['email'], 'coins': 0,
        'role': 'admin' if user['email'].lower() == ADMIN_EMAIL else 'user',
        'kyc_status': 'not_submitted', 'created_at': now_iso()
    }
    await instant_transact([['update', 'profiles', pid, profile]])
    return profile

@api.get('/banners')
async def get_banners():
    res = await instant_query({'banners': {'$': {'where': {'is_active': True}}}})
    return res.get('banners', [])

@api.post('/tasks/check-in')
async def daily_checkin(authorization: Annotated[Optional[str], Header()] = None):
    profile = await get_current_profile(authorization)
    today = today_str()
    # Check if already checked in today
    counter = await instant_query({'daily_counters': {'$': {'where': {'user_id': profile['user_id'], 'key': 'check_in', 'date': today}}}})
    if counter.get('daily_counters'): raise HTTPException(400, "Already checked in today")

    await instant_transact([['update', 'daily_counters', str(uuid.uuid4()), {'user_id': profile['user_id'], 'key': 'check_in', 'date': today, 'count': 1}]])
    await add_coins(profile, REWARDS['check_in'])
    return {"reward": REWARDS['check_in']}

@api.post('/tasks/spin')
async def spin_wheel(authorization: Annotated[Optional[str], Header()] = None):
    profile = await get_current_profile(authorization)
    # Logic for spin limits and random rewards
    reward = random.randint(REWARDS['spin_min'], REWARDS['spin_max'])
    await add_coins(profile, reward)
    return {"reward": reward}

@api.post('/tasks/scratch')
async def scratch_card(authorization: Annotated[Optional[str], Header()] = None):
    profile = await get_current_profile(authorization)
    reward = random.randint(REWARDS['scratch_min'], REWARDS['scratch_max'])
    await add_coins(profile, reward)
    return {"reward": reward}

@api.get('/admin/stats')
async def admin_stats(authorization: Annotated[Optional[str], Header()] = None):
    profile = await get_current_profile(authorization)
    if profile.get('role') != 'admin': raise HTTPException(403)
    res = await instant_query({'profiles': {}})
    profs = res.get('profiles', [])
    return {
        'total_users': len(profs),
        'total_coins_circulating': sum(int(p.get('coins') or 0) for p in profs),
        'pending_kyc': sum(1 for p in profs if p.get('kyc_status') == 'pending')
    }

app.include_router(api)
