"""
POS API services: IIKO and Dooglys — async, function-based.

Public interface
────────────────
  iiko_auth(config)                              → str | None
  iiko_get_guests_count(config, date_from, date_to, iiko_org_id) → int
  iiko_get_guests_for_branches(config, branches, date_from, date_to) → dict[str, int]

  dooglys_get_guests_count(config, date_from, date_to, sale_point_id) → int
  dooglys_get_guests_for_branches(config, branches, date_from, date_to) → dict[str, int]

  get_guests_for_period(config, date_from, date_to, branches)  → dict[str, int]
  get_guests_today(config, branches)                           → dict[str, int]

Field mapping (ClientConfig model):
  iiko_api_url, iiko_login, iiko_password
  dooglys_api_url, dooglys_api_token

Field mapping (Branch model):
  iiko_organization_id     — UUID string, matches Department.Id in IIKO OLAP
  dooglys_sale_point_id    — string/int, passed as sale_point_id to Dooglys API
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import date, datetime, timedelta
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# ── IIKO token cache ────────────────────────────────────────────────────────
# {(base_url, login): (token, expires_at)}
_iiko_token_cache: dict[tuple[str, str], tuple[str, datetime]] = {}
_IIKO_TOKEN_TTL = 840  # 14 min (IIKO tokens last 15 min)


# ============================================================================
# Helpers
# ============================================================================

def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()


def _iiko_base_url(config) -> str:
    return config.iiko_api_url.rstrip('/')


def _dooglys_base_url(config) -> str:
    raw = (getattr(config, 'dooglys_api_url', None) or 'https://dooglys.com/api/v1').rstrip('/')
    if not raw.endswith('/api/v1'):
        raw += '/api/v1'
    return raw


def _day_bounds(target: date) -> tuple[str, str]:
    """Return (start_str, end_str) for a full calendar day, formatted for Dooglys."""
    start = datetime(target.year, target.month, target.day, 0, 0, 0)
    end = datetime(target.year, target.month, target.day, 23, 59, 59)
    return start.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S')


# ============================================================================
# IIKO — authentication (with in-process cache)
# ============================================================================

async def iiko_auth(config) -> Optional[str]:
    """
    Authenticate against IIKO API and return a session token.

    Token is cached in-process for _IIKO_TOKEN_TTL seconds so that parallel
    branch fetches reuse the same connection/token without hitting the IIKO
    license connection limit.

    Returns None if config is missing required fields or auth fails.
    """
    if not (getattr(config, 'iiko_api_url', None) and getattr(config, 'iiko_login', None)):
        logger.warning('iiko_auth: iiko_api_url or iiko_login not set')
        return None

    base_url = _iiko_base_url(config)
    login = config.iiko_login
    password = config.iiko_password or ''
    cache_key = (base_url, login)
    now = datetime.utcnow()

    # Check cache
    if cache_key in _iiko_token_cache:
        token, expires_at = _iiko_token_cache[cache_key]
        if now < expires_at:
            logger.debug('iiko_auth: using cached token (expires in %ds)', (expires_at - now).seconds)
            return token
        del _iiko_token_cache[cache_key]

    url = f'{base_url}/resto/api/auth'
    params = {'login': login, 'pass': _sha1(password)}

    try:
        async with httpx.AsyncClient(verify=False, timeout=15) as client:
            resp = await client.get(url, params=params)

        if resp.status_code == 200:
            token = resp.text.strip()
            _iiko_token_cache[cache_key] = (token, now + timedelta(seconds=_IIKO_TOKEN_TTL))
            logger.debug('iiko_auth: new token cached for %ds', _IIKO_TOKEN_TTL)
            return token

        logger.error('iiko_auth failed: %s %s', resp.status_code, resp.text[:200])

    except httpx.RequestError as exc:
        logger.error('iiko_auth connection error: %s', exc)

    return None


# ============================================================================
# IIKO — guest counts
# ============================================================================

async def iiko_get_guests_count(
    config,
    date_from: date,
    date_to: date,
    iiko_org_id: Optional[str] = None,
    *,
    token: Optional[str] = None,
) -> int:
    """
    Fetch number of unique orders (=checks) from IIKO OLAP for a period.

    Args:
        config:      ClientConfig instance.
        date_from:   Start of period (inclusive).
        date_to:     End of period (inclusive).
        iiko_org_id: Department.Id (UUID) from Branch.iiko_organization_id.
                     If None — sums all departments (single-branch use case).
        token:       Pre-obtained auth token; obtained automatically if not provided.

    Returns:
        Number of unique orders, or 0 on any error.
    """
    if not getattr(config, 'iiko_api_url', None):
        return 0

    if token is None:
        token = await iiko_auth(config)
    if not token:
        return 0

    base_url = _iiko_base_url(config)
    body: dict[str, Any] = {
        'reportType': 'SALES',
        'buildSummary': 'false',
        'groupByRowFields': ['Department', 'Department.Id'],
        'groupByColFields': [],
        'aggregateFields': ['UniqOrderId.OrdersCount'],
        'filters': {
            'OpenDate.Typed': {
                'filterType': 'DateRange',
                'periodType': 'CUSTOM',
                'from': date_from.strftime('%Y-%m-%d'),
                'to': date_to.strftime('%Y-%m-%d'),
                'includeLow': True,
                'includeHigh': True,
            }
        },
    }

    try:
        async with httpx.AsyncClient(verify=False, timeout=30) as client:
            resp = await client.post(
                f'{base_url}/resto/api/v2/reports/olap',
                params={'key': token},
                json=body,
                headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
            )

        if resp.status_code != 200:
            logger.error('iiko_get_guests_count → %s: %s', resp.status_code, resp.text[:200])
            return 0

        data = resp.json()

    except (httpx.RequestError, ValueError) as exc:
        logger.error('iiko_get_guests_count error: %s', exc)
        return 0

    if 'data' not in data:
        logger.warning('iiko_get_guests_count: no "data" key in response')
        return 0

    total = 0
    for row in data['data']:
        dept_id = row.get('Department.Id', '')
        count = int(row.get('UniqOrderId.OrdersCount', 0) or 0)
        if iiko_org_id and dept_id != iiko_org_id:
            continue
        logger.debug('iiko row: dept_id=%s count=%d', dept_id, count)
        total += count

    return total


async def iiko_get_guests_for_branches(
    config,
    branches,
    date_from: date,
    date_to: date,
) -> dict[str, int]:
    """
    Fetch guest counts for all branches concurrently (one request per branch).

    Args:
        config:    ClientConfig instance.
        branches:  Iterable of Branch instances with iiko_organization_id field.
        date_from: Start of period (inclusive).
        date_to:   End of period (inclusive).

    Returns:
        {iiko_organization_id: guest_count}
        Branches without iiko_organization_id are skipped.
    """
    # Auth once; reuse token for all parallel requests
    token = await iiko_auth(config)
    if not token:
        return {}

    branch_list = [b for b in branches if getattr(b, 'iiko_organization_id', None)]
    if not branch_list:
        logger.warning('iiko_get_guests_for_branches: no branches with iiko_organization_id')
        return {}

    async def _fetch(branch) -> tuple[str, int]:
        count = await iiko_get_guests_count(
            config, date_from, date_to,
            iiko_org_id=branch.iiko_organization_id,
            token=token,
        )
        return branch.iiko_organization_id, count

    tasks = [_fetch(b) for b in branch_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    output: dict[str, int] = {}
    for branch, result in zip(branch_list, results):
        if isinstance(result, Exception):
            logger.error(
                'iiko_get_guests_for_branches: error for branch %s: %s',
                branch.iiko_organization_id, result,
            )
            output[branch.iiko_organization_id] = 0
        else:
            org_id, count = result
            output[org_id] = count

    return output


# ============================================================================
# Dooglys — guest counts
# ============================================================================

async def dooglys_get_guests_count(
    config,
    date_from: date,
    date_to: date,
    sale_point_id: Optional[str] = None,
) -> int:
    """
    Fetch number of orders from Dooglys /sales/order/list.

    The total is read from the X-Pagination-Total-Count response header
    so only a single lightweight request (per-page=1) is needed.

    Args:
        config:        ClientConfig instance.
        date_from:     Start of period (inclusive).
        date_to:       End of period (inclusive).
        sale_point_id: Branch.dooglys_sale_point_id for filtering. None = all.

    Returns:
        Number of orders, or 0 on error.
    """
    token = getattr(config, 'dooglys_api_token', None)
    if not token:
        logger.warning('dooglys_get_guests_count: dooglys_api_token not set')
        return 0

    base_url = _dooglys_base_url(config)
    start_str, end_str = _day_bounds(date_from)
    # For multi-day ranges reuse the start of date_from and the end of date_to
    _, end_str = _day_bounds(date_to)

    params: dict[str, Any] = {
        'date_accepted_from': start_str,
        'date_accepted_to': end_str,
        'per-page': 1,
        'page': 1,
    }
    if sale_point_id:
        params['sale_point_id'] = sale_point_id

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Access-Token': token,
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f'{base_url}/sales/order/list',
                params=params,
                headers=headers,
            )

        if resp.status_code != 200:
            logger.error(
                'dooglys_get_guests_count → %s: %s',
                resp.status_code, resp.text[:200],
            )
            return 0

        total_str = (
            resp.headers.get('X-Pagination-Total-Count')
            or resp.headers.get('x-pagination-total-count')
            or '0'
        )
        logger.debug(
            'dooglys_get_guests_count: sale_point_id=%s total=%s period=%s–%s',
            sale_point_id, total_str, date_from, date_to,
        )
        return int(total_str)

    except (httpx.RequestError, ValueError) as exc:
        logger.error('dooglys_get_guests_count error: %s', exc)
        return 0


async def dooglys_get_guests_for_branches(
    config,
    branches,
    date_from: date,
    date_to: date,
) -> dict[str, int]:
    """
    Fetch guest counts for all branches concurrently (one request per branch).

    Args:
        config:    ClientConfig instance.
        branches:  Iterable of Branch instances with dooglys_sale_point_id field.
        date_from: Start of period (inclusive).
        date_to:   End of period (inclusive).

    Returns:
        {dooglys_sale_point_id (str): guest_count}
        Branches without dooglys_sale_point_id are skipped.
    """
    branch_list = [b for b in branches if getattr(b, 'dooglys_sale_point_id', None)]
    if not branch_list:
        logger.warning('dooglys_get_guests_for_branches: no branches with dooglys_sale_point_id')
        return {}

    async def _fetch(branch) -> tuple[str, int]:
        count = await dooglys_get_guests_count(
            config, date_from, date_to,
            sale_point_id=str(branch.dooglys_sale_point_id),
        )
        return str(branch.dooglys_sale_point_id), count

    tasks = [_fetch(b) for b in branch_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    output: dict[str, int] = {}
    for branch, result in zip(branch_list, results):
        sp_id = str(branch.dooglys_sale_point_id)
        if isinstance(result, Exception):
            logger.error(
                'dooglys_get_guests_for_branches: error for branch %s: %s',
                sp_id, result,
            )
            output[sp_id] = 0
        else:
            _, count = result
            output[sp_id] = count

    return output


# ============================================================================
# Unified facade
# ============================================================================

async def get_guests_for_period(
    config,
    date_from: date,
    date_to: date,
    branches=None,
) -> dict[str, int]:
    """
    Fetch guest/order counts for a period from the configured POS system.

    Dispatches to the correct POS implementation based on config.pos_type.
    All branch requests are executed concurrently.

    Args:
        config:    ClientConfig instance.
        date_from: Start of period (inclusive).
        date_to:   End of period (inclusive).
        branches:  Iterable of Branch instances. If None — returns a single
                   aggregate count under key "__all__".

    Returns:
        For IIKO:    {iiko_organization_id: count, ...}
        For Dooglys: {dooglys_sale_point_id: count, ...}
        On error / POS not configured: {}
    """
    from apps.shared.config.models import POSType  # avoid circular import at module level

    pos = getattr(config, 'pos_type', POSType.NONE)

    if pos == POSType.IIKO:
        if branches is not None:
            return await iiko_get_guests_for_branches(config, branches, date_from, date_to)
        # No branches provided — fetch aggregate total
        total = await iiko_get_guests_count(config, date_from, date_to)
        return {'__all__': total} if total else {}

    if pos == POSType.DOOGLYS:
        if branches is not None:
            return await dooglys_get_guests_for_branches(config, branches, date_from, date_to)
        total = await dooglys_get_guests_count(config, date_from, date_to)
        return {'__all__': total} if total else {}

    logger.info('get_guests_for_period: pos_type=%s — not configured', pos)
    return {}


async def get_guests_today(
    config,
    branches=None,
) -> dict[str, int]:
    """
    Convenience wrapper: fetch guest counts for TODAY.

    Returns same structure as get_guests_for_period.
    """
    today = date.today()
    return await get_guests_for_period(config, today, today, branches=branches)


# ============================================================================
# Sync wrappers (for use in Celery tasks / Django ORM context)
# ============================================================================

def sync_get_guests_for_period(
    config,
    date_from: date,
    date_to: date,
    branches=None,
) -> dict[str, int]:
    """
    Synchronous wrapper around get_guests_for_period.

    Use this from Celery tasks or anywhere that cannot await coroutines.
    Creates a new event loop if one is not already running.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # We are inside an already-running loop (e.g. async Django view).
            # Spin up a background thread with its own event loop.
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    asyncio.run,
                    get_guests_for_period(config, date_from, date_to, branches=branches),
                )
                return future.result()
        return loop.run_until_complete(
            get_guests_for_period(config, date_from, date_to, branches=branches)
        )
    except RuntimeError:
        return asyncio.run(
            get_guests_for_period(config, date_from, date_to, branches=branches)
        )


def sync_get_guests_today(config, branches=None) -> dict[str, int]:
    """Synchronous wrapper around get_guests_today."""
    today = date.today()
    return sync_get_guests_for_period(config, today, today, branches=branches)
