"""Coin Earn — InstantDB-backed FastAPI server.

All persistent state lives in InstantDB. This service acts as a trusted
mutator: it verifies the caller via their InstantDB refresh token, then
performs atomic, idempotent writes using the InstantDB HTTP Admin API.

Entities:
  profiles      -> id, user_id, email, role, coins, upi_id, kyc_status,
                   kyc_doc_url, quiz_streak, created_at
  withdrawals   -> id, user_id, email, amount_coins, inr_amount, upi_id,
                   status, created_at
  daily_counters-> id, user_id, key, date, count   (composite-key idempotency)
  quiz_questions-> id, user_id, answer, used, created_at
"""
from fastapi import FastAPI, APIRouter, HTTPException, Depends, Header
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
import os
import logging
import random
import uuid
import httpx
import asyncio
from collections import defaultdict
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Optional, Annotated, Any, Dict, List
from datetime import datetime, timezone
from contextlib import asynccontextmanager

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

INSTANT_APP_ID = os.environ['INSTANT_APP_ID']
INSTANT_ADMIN_TOKEN = os.environ['INSTANT_ADMIN_TOKEN']
ADMIN_EMAIL = os.environ['ADMIN_EMAIL'].lower()
INSTANT_BASE = 'https://api.instantdb.com'

REWARDS = {
    'check_in': 100,
    'spin_min': 20, 'spin_max': 50,
    'scratch_min': 20, 'scratch_max': 40,
    'watch': 80,
    'quiz': 50,
}
WITHDRAW_THRESHOLD = 300_000
CONVERSION_RATE = 1000  # coins per 1 INR
SPIN_MAX = 10
SCRATCH_MAX = 5
WATCH_MAX = 10
QUIZ_STREAK = 5

# Settings singleton id in InstantDB (fixed UUID — InstantDB requires UUID ids)
SETTINGS_ID = '00000000-0000-0000-0000-000000005e77'  # 'sett' in leet ;)
# In-process cache (refreshed on update + on startup)
_settings_cache: Dict[str, Any] = {}


def _defaults() -> Dict[str, Any]:
    return {
        'conversion_rate': CONVERSION_RATE,
        'withdraw_threshold': WITHDRAW_THRESHOLD,
        'check_in': REWARDS['check_in'],
        'spin_min': REWARDS['spin_min'],
        'spin_max': REWARDS['spin_max'],
        'scratch_min': REWARDS['scratch_min'],
        'scratch_max': REWARDS['scratch_max'],
        'watch': REWARDS['watch'],
        'quiz': REWARDS['quiz'],
        'updated_at': '',
    }


def s(key: str) -> int:
    """Get a setting value (cached, with fallback to default)."""
    v = _settings_cache.get(key)
    if v is None:
        v = _defaults().get(key)
    return int(v) if isinstance(v, (int, float)) else v

# Referral milestones: count -> coins awarded
REFERRAL_MILESTONES: Dict[int, int] = {
    1: 500,
    5: 2_000,
    10: 5_000,
    25: 15_000,
    50: 50_000,
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('coin-earn')

# Per-user asyncio locks for serializing sensitive operations (withdraw).
# Single-process FastAPI guarantee — concurrent calls from same user serialize.
_user_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


# -------------------- InstantDB HTTP Admin client --------------------
def _headers(as_token: Optional[str] = None) -> Dict[str, str]:
    h = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {INSTANT_ADMIN_TOKEN}',
        'App-Id': INSTANT_APP_ID,
    }
    if as_token:
        h['As-Token'] = as_token
    return h


async def instant_query(query: Dict[str, Any], as_token: Optional[str] = None) -> Dict[str, Any]:
    async with httpx.AsyncClient(base_url=INSTANT_BASE, timeout=15.0) as c:
        r = await c.post('/admin/query', headers=_headers(as_token), json={'query': query})
        if r.status_code >= 400:
            logger.error(f'Instant query failed [{r.status_code}]: {r.text}')
            raise HTTPException(status_code=r.status_code, detail=f'Instant query: {r.text}')
        return r.json()


async def instant_transact(steps: List[List[Any]], as_token: Optional[str] = None) -> Dict[str, Any]:
    async with httpx.AsyncClient(base_url=INSTANT_BASE, timeout=15.0) as c:
        r = await c.post('/admin/transact', headers=_headers(as_token), json={'steps': steps})
        if r.status_code >= 400:
            logger.error(f'Instant transact failed [{r.status_code}]: {r.text}')
            raise HTTPException(status_code=r.status_code, detail=f'Instant transact: {r.text}')
        return r.json()


# -------------------- helpers --------------------
def today_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def get_auth_user(token: str) -> Dict[str, Any]:
    """Verify caller's InstantDB refresh token by issuing an As-Token query.
    Returns the $users record (id, email). With proper perms in place,
    `$users` should only ever return the caller's own row."""
    data = await instant_query({'$users': {}}, as_token=token)
    users = data.get('$users', [])
    if not users:
        raise HTTPException(status_code=401, detail='Invalid token')
    if len(users) != 1:
        # Defensive: perms misconfiguration could leak other users; refuse.
        logger.error(f'Auth resolution leaked {len(users)} users — perms likely too permissive on $users')
        raise HTTPException(status_code=500, detail='Auth resolution ambiguous')
    return users[0]


def gen_referral_code(email: str) -> str:
    base = ''.join(c for c in email.split('@')[0].upper() if c.isalnum())[:4] or 'USER'
    suffix = uuid.uuid4().hex[:4].upper()
    return f'{base}{suffix}'


async def get_or_create_profile(user: Dict[str, Any]) -> Dict[str, Any]:
    """Find profile by user_id; create if missing. Auto-promote admin email."""
    user_id = user['id']
    email = (user.get('email') or '').lower()
    res = await instant_query({'profiles': {'$': {'where': {'user_id': user_id}}}})
    rows = res.get('profiles', [])
    if rows:
        return rows[0]

    role = 'admin' if email == ADMIN_EMAIL else 'user'
    profile_id = str(uuid.uuid4())
    doc = {
        'user_id': user_id,
        'email': email,
        'role': role,
        'coins': 0,
        'upi_id': None,
        'kyc_status': 'not_submitted',
        'kyc_legal_name': None,
        'kyc_doc_url': None,
        'quiz_streak': 0,
        'referral_code': gen_referral_code(email),
        'referred_by_code': None,
        'referrals_count': 0,
        'created_at': now_iso(),
    }
    await instant_transact([['update', 'profiles', profile_id, doc]])
    doc['id'] = profile_id
    return doc


async def get_profile_for_token(x_instant_token: str) -> Dict[str, Any]:
    user = await get_auth_user(x_instant_token)
    return await get_or_create_profile(user)


async def current_profile(
    x_instant_token: Annotated[Optional[str], Header(alias='X-Instant-Token')] = None,
) -> Dict[str, Any]:
    if not x_instant_token:
        raise HTTPException(status_code=401, detail='Missing X-Instant-Token')
    return await get_profile_for_token(x_instant_token)


async def require_admin(profile: Annotated[Dict[str, Any], Depends(current_profile)]) -> Dict[str, Any]:
    if profile.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='Admin only')
    return profile


async def update_profile(profile_id: str, fields: Dict[str, Any]) -> None:
    await instant_transact([['update', 'profiles', profile_id, fields]])


# -------------------- Settings helpers --------------------
async def load_settings_into_cache() -> Dict[str, Any]:
    res = await instant_query({'settings': {'$': {'where': {'id': SETTINGS_ID}}}})
    rows = res.get('settings', [])
    if rows:
        _settings_cache.update(rows[0])
        return rows[0]
    # First run — seed the row with defaults
    seed = _defaults() | {'updated_at': now_iso()}
    await instant_transact([['update', 'settings', SETTINGS_ID, seed]])
    _settings_cache.update(seed)
    return seed


async def save_settings(patch: Dict[str, Any], admin_id: str) -> Dict[str, Any]:
    # Coerce numerics
    for k in ('conversion_rate', 'withdraw_threshold', 'check_in',
             'spin_min', 'spin_max', 'scratch_min', 'scratch_max', 'watch', 'quiz'):
        if k in patch:
            patch[k] = int(patch[k])
    patch['updated_at'] = now_iso()
    patch['updated_by'] = admin_id
    await instant_transact([['update', 'settings', SETTINGS_ID, patch]])
    _settings_cache.update(patch)
    return _settings_cache.copy()


async def add_coins(profile: Dict[str, Any], amount: int) -> int:
    current_coins = profile.get('coins')
    # Force conversion to int to avoid type errors in DB
    try:
        current_coins = int(current_coins) if current_coins is not None else 0
    except (ValueError, TypeError):
        current_coins = 0

    new_coins = current_coins + amount
    await update_profile(profile['id'], {'coins': new_coins})
    return new_coins


async def get_daily_count(user_id: str, key: str) -> int:
    today = today_str()
    res = await instant_query({
        'daily_counters': {'$': {'where': {'user_id': user_id, 'key': key, 'date': today}}}
    })
    rows = res.get('daily_counters', [])
    return int(rows[0]['count']) if rows else 0


async def inc_daily_count(user_id: str, key: str) -> int:
    today = today_str()
    res = await instant_query({
        'daily_counters': {'$': {'where': {'user_id': user_id, 'key': key, 'date': today}}}
    })
    rows = res.get('daily_counters', [])
    if rows:
        row = rows[0]
        new = int(row['count']) + 1
        await instant_transact([['update', 'daily_counters', row['id'], {'count': new}]])
        return new
    cid = str(uuid.uuid4())
    await instant_transact([['update', 'daily_counters', cid, {
        'user_id': user_id, 'key': key, 'date': today, 'count': 1, 'created_at': now_iso(),
    }]])
    return 1


# -------------------- lifespan --------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Connectivity smoke test
        await instant_query({'profiles': {'$': {'limit': 1}}})
        await load_settings_into_cache()
        logger.info(f'InstantDB connected. Admin email: {ADMIN_EMAIL}. Settings loaded.')
    except Exception as e:
        logger.error(f'InstantDB startup ping failed: {e}')
    yield


app = FastAPI(lifespan=lifespan)
api = APIRouter(prefix='/api')

@app.get("/")
async def root_health_check():
    return {"status": "ok", "message": "Coin Earn Backend is Running on Vercel"}


# -------------------- Pydantic models --------------------
class UpiIn(BaseModel):
    upi_id: str = Field(min_length=3)


class KycIn(BaseModel):
    legal_name: str = Field(min_length=2)
    kyc_doc_url: str = Field(min_length=8)  # Cloudinary URL


class QuizAnswerIn(BaseModel):
    question_id: str
    answer: int


class KycReviewIn(BaseModel):
    status: str


class ReferralIn(BaseModel):
    code: str = Field(min_length=4, max_length=16)


class CoinAdjustIn(BaseModel):
    delta: int
    reason: str = Field(min_length=2, max_length=200)


class BanIn(BaseModel):
    banned: bool
    reason: Optional[str] = None


class WithdrawalReviewIn(BaseModel):
    status: str  # 'approved' | 'rejected'
    note: Optional[str] = None


class SettingsIn(BaseModel):
    conversion_rate: Optional[int] = None
    withdraw_threshold: Optional[int] = None
    check_in: Optional[int] = None
    spin_min: Optional[int] = None
    spin_max: Optional[int] = None
    scratch_min: Optional[int] = None
    scratch_max: Optional[int] = None
    watch: Optional[int] = None
    quiz: Optional[int] = None


def public_profile(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'id': p['id'],
        'user_id': p.get('user_id'),
        'email': p.get('email'),
        'role': p.get('role', 'user'),
        'coins': int(p.get('coins') or 0),
        'upi_id': p.get('upi_id'),
        'kyc_status': p.get('kyc_status', 'not_submitted'),
        'kyc_legal_name': p.get('kyc_legal_name'),
        'kyc_doc_url': p.get('kyc_doc_url'),
        'quiz_streak': int(p.get('quiz_streak') or 0),
        'referral_code': p.get('referral_code'),
        'referred_by_code': p.get('referred_by_code'),
        'referrals_count': int(p.get('referrals_count') or 0),
    }


async def ensure_referral_fields(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Backfill referral fields on existing profiles created before the feature."""
    patch: Dict[str, Any] = {}
    if not profile.get('referral_code'):
        patch['referral_code'] = gen_referral_code(profile.get('email') or 'user')
    if profile.get('referrals_count') is None:
        patch['referrals_count'] = 0
    if patch:
        await update_profile(profile['id'], patch)
        profile.update(patch)
    return profile


# -------------------- Routes --------------------
@api.get('/')
async def root():
    return {'message': 'Coin Earn API (InstantDB)', 'status': 'ok'}


@api.post('/profile/bootstrap')
async def bootstrap(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    """Called by frontend after magic-code login. Creates/returns the profile
    and promotes admin if email matches ADMIN_EMAIL."""
    profile = await ensure_referral_fields(profile)
    return public_profile(profile)


@api.get('/profile/me')
async def me(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    return public_profile(profile)


# KYC: store Cloudinary URL submitted by client
@api.post('/kyc/submit')
async def kyc_submit(payload: KycIn, profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    if profile.get('kyc_status') == 'approved':
        raise HTTPException(status_code=400, detail='KYC already approved')
    await update_profile(profile['id'], {
        'kyc_status': 'pending',
        'kyc_legal_name': payload.legal_name,
        'kyc_doc_url': payload.kyc_doc_url,
        'kyc_submitted_at': now_iso(),
    })
    return {'status': 'pending'}


# ---------- Tasks ----------
@api.post('/tasks/check-in')
async def task_checkin(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    if await get_daily_count(profile['user_id'], 'check_in') > 0:
        raise HTTPException(status_code=400, detail='Already checked in today')
    await inc_daily_count(profile['user_id'], 'check_in')
    reward = s('check_in')
    coins = await add_coins(profile, reward)
    return {'reward': reward, 'coins': coins}


@api.get('/tasks/spin/status')
async def spin_status(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    used = await get_daily_count(profile['user_id'], 'spin')
    return {'spins_used': used, 'spins_max': SPIN_MAX, 'spins_remaining': max(0, SPIN_MAX - used)}


@api.post('/tasks/spin')
async def task_spin(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    used = await get_daily_count(profile['user_id'], 'spin')
    if used >= SPIN_MAX:
        raise HTTPException(status_code=400, detail=f'Daily spin limit reached ({SPIN_MAX})')
    new_used = await inc_daily_count(profile['user_id'], 'spin')
    reward = random.randint(s('spin_min'), s('spin_max'))
    coins = await add_coins(profile, reward)
    return {'reward': reward, 'coins': coins, 'spins_used': new_used, 'spins_max': SPIN_MAX}


@api.get('/tasks/scratch/status')
async def scratch_status(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    used = await get_daily_count(profile['user_id'], 'scratch')
    return {'scratches_used': used, 'scratches_max': SCRATCH_MAX, 'scratches_remaining': max(0, SCRATCH_MAX - used)}


@api.post('/tasks/scratch')
async def task_scratch(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    used = await get_daily_count(profile['user_id'], 'scratch')
    if used >= SCRATCH_MAX:
        raise HTTPException(status_code=400, detail=f'Daily scratch limit reached ({SCRATCH_MAX})')
    new_used = await inc_daily_count(profile['user_id'], 'scratch')
    reward = random.randint(s('scratch_min'), s('scratch_max'))
    coins = await add_coins(profile, reward)
    return {'reward': reward, 'coins': coins, 'scratches_used': new_used, 'scratches_max': SCRATCH_MAX}


@api.get('/tasks/watch/status')
async def watch_status(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    used = await get_daily_count(profile['user_id'], 'watch')
    return {'watches_used': used, 'watches_max': WATCH_MAX, 'watches_remaining': max(0, WATCH_MAX - used)}


@api.post('/tasks/watch')
async def task_watch(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    used = await get_daily_count(profile['user_id'], 'watch')
    if used >= WATCH_MAX:
        raise HTTPException(status_code=400, detail=f'Daily watch limit reached ({WATCH_MAX})')
    new_used = await inc_daily_count(profile['user_id'], 'watch')
    reward = s('watch')
    coins = await add_coins(profile, reward)
    return {'reward': reward, 'coins': coins, 'watches_used': new_used, 'watches_max': WATCH_MAX}


# Quiz: server holds answer
@api.get('/tasks/quiz/new')
async def quiz_new(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    op = random.choice(['+', '-', '*'])
    if op == '*':
        a, b = random.randint(2, 9), random.randint(2, 9)
        ans = a * b
    elif op == '-':
        a, b = random.randint(5, 25), random.randint(2, 20)
        if b > a:
            a, b = b, a
        ans = a - b
    else:
        a, b = random.randint(2, 25), random.randint(2, 25)
        ans = a + b
    qid = str(uuid.uuid4())
    await instant_transact([['update', 'quiz_questions', qid, {
        'user_id': profile['user_id'], 'answer': ans, 'used': False, 'created_at': now_iso(),
    }]])
    return {
        'question_id': qid,
        'question': f'{a} {op} {b}',
        'streak': int(profile.get('quiz_streak') or 0),
        'target': QUIZ_STREAK,
    }


@api.post('/tasks/quiz/answer')
async def quiz_answer(payload: QuizAnswerIn, profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    res = await instant_query({
        'quiz_questions': {'$': {'where': {'id': payload.question_id, 'user_id': profile['user_id']}}}
    })
    rows = res.get('quiz_questions', [])
    if not rows:
        raise HTTPException(status_code=404, detail='Question not found')
    q = rows[0]
    if q.get('used'):
        raise HTTPException(status_code=400, detail='Already answered')
    await instant_transact([['update', 'quiz_questions', q['id'], {'used': True}]])
    correct = int(payload.answer) == int(q['answer'])
    streak = int(profile.get('quiz_streak') or 0)
    coins = int(profile.get('coins') or 0)
    if not correct:
        await update_profile(profile['id'], {'quiz_streak': 0})
        return {'correct': False, 'streak': 0, 'target': QUIZ_STREAK, 'reward': 0, 'coins': coins}
    new_streak = streak + 1
    if new_streak >= QUIZ_STREAK:
        reward = REWARDS['quiz']
        await update_profile(profile['id'], {'quiz_streak': 0, 'coins': coins + reward})
        return {'correct': True, 'streak': 0, 'target': QUIZ_STREAK, 'reward': reward, 'coins': coins + reward, 'completed': True}
    await update_profile(profile['id'], {'quiz_streak': new_streak})
    return {'correct': True, 'streak': new_streak, 'target': QUIZ_STREAK, 'reward': 0, 'coins': coins}


@api.get('/tasks/quiz/status')
async def quiz_status(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    return {'streak': int(profile.get('quiz_streak') or 0), 'target': QUIZ_STREAK}


# ---------- Custom Tasks ----------
@api.post('/tasks/custom/{task_id}/claim')
async def claim_custom_task(task_id: str, profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    user_id = profile['user_id']
    async with _user_locks[user_id]:
        fresh_res = await instant_query({'profiles': {'$': {'where': {'user_id': user_id}}}})
        fresh = fresh_res.get('profiles', [])[0]
        
        # Check if task exists and is active
        task_res = await instant_query({'custom_tasks': {'$': {'where': {'id': task_id}}}})
        tasks = task_res.get('custom_tasks', [])
        if not tasks:
            raise HTTPException(status_code=404, detail='Task not found')
        task = tasks[0]
        if not task.get('is_active'):
            raise HTTPException(status_code=400, detail='Task is no longer active')
            
        # Check if already completed
        comp_res = await instant_query({
            'custom_task_completions': {'$': {'where': {'user_id': user_id, 'task_id': task_id}}}
        })
        if comp_res.get('custom_task_completions'):
            raise HTTPException(status_code=400, detail='Already claimed this task')
            
        reward = int(task.get('reward_coins') or 0)
        new_coins = int(fresh.get('coins') or 0) + reward
        
        await instant_transact([
            ['update', 'profiles', fresh['id'], {'coins': new_coins}],
            ['update', 'custom_task_completions', str(uuid.uuid4()), {
                'user_id': user_id,
                'task_id': task_id,
                'created_at': now_iso(),
            }],
        ])
        
        return {'reward': reward, 'coins': new_coins}


# ---------- Wallet ----------
@api.post('/wallet/upi')
async def set_upi(payload: UpiIn, profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    await update_profile(profile['id'], {'upi_id': payload.upi_id.strip()})
    return {'upi_id': payload.upi_id.strip()}


@api.post('/wallet/withdraw')
async def withdraw(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    user_id = profile['user_id']
    if profile.get('banned'):
        raise HTTPException(status_code=403, detail='Withdrawals are suspended for this account')
    async with _user_locks[user_id]:
        fresh_res = await instant_query({'profiles': {'$': {'where': {'user_id': user_id}}}})
        rows = fresh_res.get('profiles', [])
        if not rows:
            raise HTTPException(status_code=404, detail='Profile not found')
        fresh = rows[0]
        if fresh.get('banned'):
            raise HTTPException(status_code=403, detail='Withdrawals are suspended for this account')
        coins = int(fresh.get('coins') or 0)
        threshold = s('withdraw_threshold')
        rate = s('conversion_rate')
        if coins < threshold:
            raise HTTPException(status_code=400, detail=f'Need {threshold} coins')
        if not fresh.get('upi_id'):
            raise HTTPException(status_code=400, detail='UPI ID not set')
        if fresh.get('kyc_status') != 'approved':
            raise HTTPException(status_code=400, detail='KYC not approved')
        inr_amount = threshold // rate
        wid = str(uuid.uuid4())
        new_coins = coins - threshold
        await instant_transact([
            ['update', 'profiles', fresh['id'], {'coins': new_coins}],
            ['update', 'withdrawals', wid, {
                'user_id': user_id,
                'email': fresh.get('email'),
                'amount_coins': threshold,
                'inr_amount': inr_amount,
                'upi_id': fresh['upi_id'],
                'status': 'requested',
                'created_at': now_iso(),
            }],
        ])
        return {'id': wid, 'inr_amount': inr_amount, 'coins': new_coins, 'status': 'requested'}


# ---------- Referrals ----------
async def _award_referral_milestones(referrer_profile_id: str, referrer_user_id: str,
                                     referrer_email: str, new_count: int, current_coins: int) -> int:
    """Check which milestones the referrer just crossed, award all coins +
    create all milestone rows in **a single InstantDB transact**.
    Returns total coins delta added.
    """
    # 1) Fetch already-unlocked milestones once (avoid N queries inside the loop)
    res = await instant_query({
        'referral_milestones': {'$': {'where': {'user_id': referrer_user_id}}}
    })
    already_unlocked = {int(m['milestone']) for m in res.get('referral_milestones', [])}

    # 2) Compute newly-crossed milestones in one pass
    steps: List[List[Any]] = []
    delta = 0
    for milestone, reward in sorted(REFERRAL_MILESTONES.items()):
        if new_count < milestone:
            break
        if milestone in already_unlocked:
            continue
        delta += reward
        steps.append(['update', 'referral_milestones', str(uuid.uuid4()), {
            'user_id': referrer_user_id,
            'email': referrer_email,
            'milestone': milestone,
            'coins_awarded': reward,
            'created_at': now_iso(),
        }])

    if not steps:
        return 0

    # 3) One profile coin update for all milestones combined
    steps.append(['update', 'profiles', referrer_profile_id, {
        'coins': current_coins + delta,
    }])
    await instant_transact(steps)
    return delta


@api.get('/referral/me')
async def referral_me(profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    profile = await ensure_referral_fields(profile)
    milestones = await instant_query({
        'referral_milestones': {'$': {'where': {'user_id': profile['user_id']}}}
    })
    return {
        'referral_code': profile.get('referral_code'),
        'referred_by_code': profile.get('referred_by_code'),
        'referrals_count': int(profile.get('referrals_count') or 0),
        'milestones': REFERRAL_MILESTONES,
        'unlocked': [m['milestone'] for m in milestones.get('referral_milestones', [])],
    }


@api.post('/referral/apply')
async def referral_apply(payload: ReferralIn, profile: Annotated[Dict[str, Any], Depends(current_profile)]):
    profile = await ensure_referral_fields(profile)
    if profile.get('referred_by_code'):
        raise HTTPException(status_code=400, detail='Referral code already applied')
    code = payload.code.strip().upper()
    if code == (profile.get('referral_code') or '').upper():
        raise HTTPException(status_code=400, detail='Cannot use your own code')

    # Find referrer
    res = await instant_query({'profiles': {'$': {'where': {'referral_code': code}}}})
    refs = res.get('profiles', [])
    if not refs:
        raise HTTPException(status_code=404, detail='Invalid referral code')
    referrer = refs[0]
    if referrer.get('user_id') == profile.get('user_id'):
        raise HTTPException(status_code=400, detail='Cannot use your own code')

    new_count = int(referrer.get('referrals_count') or 0) + 1
    referrer_coins = int(referrer.get('coins') or 0)

    # 1) Mark current user as referred by this code
    await update_profile(profile['id'], {'referred_by_code': code})
    # 2) Bump referrer's referrals_count
    await update_profile(referrer['id'], {'referrals_count': new_count})
    # 3) Award milestone(s) if any crossed
    delta = await _award_referral_milestones(
        referrer['id'], referrer['user_id'], referrer.get('email') or '', new_count, referrer_coins,
    )
    return {
        'referred_by_code': code,
        'referrer_email': referrer.get('email'),
        'referrer_new_count': new_count,
        'referrer_bonus': delta,
    }


# ---------- Admin ----------
@api.get('/admin/kyc/pending')
async def admin_kyc_pending(_admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    res = await instant_query({
        'profiles': {'$': {'where': {'kyc_status': 'pending'}}}
    })
    return [public_profile(p) | {'kyc_doc_url': p.get('kyc_doc_url')} for p in res.get('profiles', [])]


@api.post('/admin/kyc/{profile_id}')
async def admin_kyc_review(profile_id: str, payload: KycReviewIn,
                           _admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    if payload.status not in {'approved', 'rejected'}:
        raise HTTPException(status_code=400, detail='Invalid status')
    await update_profile(profile_id, {
        'kyc_status': payload.status,
        'kyc_reviewed_at': now_iso(),
    })
    return {'status': payload.status}


@api.get('/admin/withdrawals')
async def admin_withdrawals(_admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    res = await instant_query({'withdrawals': {'$': {'order': {'serverCreatedAt': 'desc'}}}})
    return res.get('withdrawals', [])


# ----- New admin features (Dashboard / Users / Withdrawals / Settings) -----
@api.get('/admin/stats')
async def admin_stats(_admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    all_profiles = await instant_query({'profiles': {}})
    profs = all_profiles.get('profiles', [])
    total_users = len(profs)
    total_coins = sum(int(p.get('coins') or 0) for p in profs)
    today = today_str()
    today_signups = sum(1 for p in profs if (p.get('created_at') or '').startswith(today))
    pending_kyc = sum(1 for p in profs if p.get('kyc_status') == 'pending')
    banned = sum(1 for p in profs if p.get('banned'))
    wres = await instant_query({'withdrawals': {'$': {'where': {'status': 'requested'}}}})
    pending_withdrawals = len(wres.get('withdrawals', []))
    return {
        'total_users': total_users,
        'today_signups': today_signups,
        'total_coins_circulating': total_coins,
        'pending_kyc': pending_kyc,
        'pending_withdrawals': pending_withdrawals,
        'banned_users': banned,
    }


@api.get('/admin/users')
async def admin_users(_admin: Annotated[Dict[str, Any], Depends(require_admin)],
                       search: str = '', limit: int = 50):
    res = await instant_query({'profiles': {'$': {'limit': max(1, min(limit, 200))}}})
    rows = res.get('profiles', [])
    if search:
        s_low = search.lower().strip()
        rows = [r for r in rows if s_low in (r.get('email') or '').lower()]
    rows.sort(key=lambda r: r.get('created_at') or '', reverse=True)
    return [public_profile(r) | {
        'banned': bool(r.get('banned') or False),
        'ban_reason': r.get('ban_reason'),
        'created_at': r.get('created_at'),
    } for r in rows]


@api.post('/admin/users/{profile_id}/adjust-coins')
async def admin_adjust_coins(profile_id: str, payload: CoinAdjustIn,
                              admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    if payload.delta == 0:
        raise HTTPException(status_code=400, detail='Delta must be non-zero')
    res = await instant_query({'profiles': {'$': {'where': {'id': profile_id}}}})
    rows = res.get('profiles', [])
    if not rows:
        raise HTTPException(status_code=404, detail='User not found')
    user_p = rows[0]
    new_coins = max(0, int(user_p.get('coins') or 0) + payload.delta)
    aid = str(uuid.uuid4())
    await instant_transact([
        ['update', 'profiles', profile_id, {'coins': new_coins}],
        ['update', 'coin_adjustments', aid, {
            'user_id': user_p['user_id'],
            'admin_id': admin['user_id'],
            'delta': payload.delta,
            'reason': payload.reason,
            'created_at': now_iso(),
        }],
    ])
    return {'coins': new_coins, 'delta': payload.delta}


@api.post('/admin/users/{profile_id}/ban')
async def admin_ban(profile_id: str, payload: BanIn,
                    _admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    patch: Dict[str, Any] = {'banned': bool(payload.banned)}
    if payload.banned:
        patch['ban_reason'] = (payload.reason or 'Suspicious activity')[:200]
    else:
        patch['ban_reason'] = None
    await update_profile(profile_id, patch)
    return {'banned': patch['banned'], 'ban_reason': patch.get('ban_reason')}


@api.get('/admin/withdrawals/pending')
async def admin_pending_withdrawals(_admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    res = await instant_query({'withdrawals': {'$': {'where': {'status': 'requested'},
                                                       'order': {'serverCreatedAt': 'desc'}}}})
    return res.get('withdrawals', [])


@api.post('/admin/withdrawals/{wid}/status')
async def admin_review_withdrawal(wid: str, payload: WithdrawalReviewIn,
                                   _admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    if payload.status not in {'approved', 'rejected'}:
        raise HTTPException(status_code=400, detail='Invalid status')
    res = await instant_query({'withdrawals': {'$': {'where': {'id': wid}}}})
    rows = res.get('withdrawals', [])
    if not rows:
        raise HTTPException(status_code=404, detail='Withdrawal not found')
    w = rows[0]
    if w.get('status') != 'requested':
        raise HTTPException(status_code=400, detail=f'Already {w.get("status")}')
    steps: List[List[Any]] = [['update', 'withdrawals', wid, {
        'status': payload.status,
        'admin_note': (payload.note or '')[:200] or None,
        'reviewed_at': now_iso(),
    }]]
    if payload.status == 'rejected':
        prof_res = await instant_query({'profiles': {'$': {'where': {'user_id': w['user_id']}}}})
        prows = prof_res.get('profiles', [])
        if prows:
            p = prows[0]
            new_coins = int(p.get('coins') or 0) + int(w.get('amount_coins') or 0)
            steps.append(['update', 'profiles', p['id'], {'coins': new_coins}])
    await instant_transact(steps)
    return {'status': payload.status, 'refunded': payload.status == 'rejected'}


@api.get('/admin/settings')
async def admin_get_settings(_admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    if not _settings_cache:
        await load_settings_into_cache()
    return _settings_cache.copy()


@api.post('/admin/settings')
async def admin_update_settings(payload: SettingsIn,
                                  admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    patch = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not patch:
        raise HTTPException(status_code=400, detail='No fields to update')
    if 'spin_min' in patch and 'spin_max' in patch and patch['spin_min'] > patch['spin_max']:
        raise HTTPException(status_code=400, detail='spin_min cannot exceed spin_max')
    if 'scratch_min' in patch and 'scratch_max' in patch and patch['scratch_min'] > patch['scratch_max']:
        raise HTTPException(status_code=400, detail='scratch_min cannot exceed scratch_max')
    if patch.get('withdraw_threshold', 1) < 1 or patch.get('conversion_rate', 1) < 1:
        raise HTTPException(status_code=400, detail='Values must be positive')
    return await save_settings(patch, admin['user_id'])


@api.get('/settings')
async def public_settings():
    if not _settings_cache:
        await load_settings_into_cache()
    return {k: v for k, v in _settings_cache.items()
            if k in {'conversion_rate', 'withdraw_threshold', 'check_in', 'spin_min',
                     'spin_max', 'scratch_min', 'scratch_max', 'watch', 'quiz'}}


# ── Banner Ads ──────────────────────────────────────────────────────
class BannerIn(BaseModel):
    title: str
    message: str
    image_url: Optional[str] = ''
    link_url: Optional[str] = ''
    is_active: bool = True
    priority: int = 0


@app.get('/banners')
async def public_banners():
    """Return only active banners, highest priority first."""
    res = await instant_query({
        'banners': {'$': {'where': {'is_active': True}, 'order': {'priority': 'desc'}, 'limit': 10}}
    })
    return res.get('banners', [])


@app.get('/admin/banners')
async def admin_list_banners(_admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    res = await instant_query({'banners': {'$': {'order': {'priority': 'desc'}}}})
    return res.get('banners', [])


@app.post('/admin/banners')
async def admin_create_banner(payload: BannerIn,
                               admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    bid = str(uuid.uuid4())
    doc = payload.model_dump() | {'id': bid, 'created_at': now_iso(), 'updated_at': now_iso()}
    await instant_transact([['update', 'banners', bid, doc]])
    return doc


@app.post('/admin/banners/{bid}')
async def admin_update_banner(bid: str, payload: BannerIn,
                               admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    res = await instant_query({'banners': {'$': {'where': {'id': bid}}}})
    rows = res.get('banners', [])
    if not rows:
        raise HTTPException(status_code=404, detail='Banner not found')
    patch = payload.model_dump() | {'updated_at': now_iso()}
    await instant_transact([['update', 'banners', bid, patch]])
    return {'id': bid, **patch}


@app.delete('/admin/banners/{bid}')
async def admin_delete_banner(bid: str, _admin: Annotated[Dict[str, Any], Depends(require_admin)]):
    res = await instant_query({'banners': {'$': {'where': {'id': bid}}}})
    rows = res.get('banners', [])
    if not rows:
        raise HTTPException(status_code=404, detail='Banner not found')
    await instant_transact([['delete', 'banners', bid]])
    return {'deleted': True}


# Mount + CORS
app.include_router(api)
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
