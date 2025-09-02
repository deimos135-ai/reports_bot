# main.py — Fiber "sales" reports-bot (summary only)

import asyncio
import html
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import BotCommand, Message, Update
from zoneinfo import ZoneInfo

# ------------------------ Settings ------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
BITRIX_WEBHOOK_BASE = os.environ["BITRIX_WEBHOOK_BASE"].rstrip("/")
WEBHOOK_BASE = os.environ["WEBHOOK_BASE"].rstrip("/")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "secret")

REPORT_TZ_NAME = os.environ.get("REPORT_TZ", "Europe/Kyiv")
REPORT_TZ = ZoneInfo(REPORT_TZ_NAME)
REPORT_TIME = os.environ.get("REPORT_TIME", "19:00")  # HH:MM

# Куди слати сумарний звіт
# - REPORT_SUMMARY_CHAT: один чат (int)
# - або REPORT_CHATS='{"all": -100...}' — fallback
_raw_report_chats = os.environ.get("REPORT_CHATS", "")
REPORT_CHATS: Dict[str, int] = json.loads(_raw_report_chats) if _raw_report_chats.strip() else {}
REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))

# ENV-переоприділення для стадій кат.0 (не обов’язково)
CAT0_EXACT_DAY_STAGE_ID = os.environ.get("CAT0_EXACT_DAY_STAGE_ID", "").strip()   # напр. C0:5
CAT0_THINK_STAGE_ID     = os.environ.get("CAT0_THINK_STAGE_ID", "").strip()       # напр. C0:DETAILS

# ------------------------ Logging -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("report_bot")

# ------------------------ App/Bot -------------------------
app = FastAPI()
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
HTTP: aiohttp.ClientSession

# ------------------------ Health --------------------------
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

# ------------------------ Bitrix helpers ------------------
async def _sleep_backoff(attempt: int, base: float = 0.5, cap: float = 8.0):
    await asyncio.sleep(min(cap, base * (2 ** attempt)))

async def b24(method: str, **params) -> Any:
    url = f"{BITRIX_WEBHOOK_BASE}/{method}.json"
    for attempt in range(6):
        try:
            async with HTTP.post(url, json=params) as resp:
                data = await resp.json()
                if "error" in data:
                    err = data["error"]; desc = data.get("error_description")
                    if err in ("QUERY_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS", "INTERNAL_SERVER_ERROR"):
                        log.warning("Bitrix transient: %s (%s), retry #%s", err, desc, attempt+1)
                        await _sleep_backoff(attempt); continue
                    raise RuntimeError(f"B24 error: {err}: {desc}")
                return data.get("result")
        except aiohttp.ClientError as e:
            log.warning("Bitrix network error: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    raise RuntimeError("Bitrix request failed after retries")

async def b24_list(method: str, *, page_size: int = 200, throttle: float = 0.15, **params) -> List[Dict[str, Any]]:
    start = 0
    out: List[Dict[str, Any]] = []
    while True:
        payload = dict(params); payload["start"] = start
        res = await b24(method, **payload)
        chunk = res or []
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk.get("items", [])
        out.extend(chunk)
        if len(chunk) < page_size: break
        start += page_size
        if throttle: await asyncio.sleep(throttle)
    return out

# ------------------------ Caches --------------------------
_DEAL_TYPE_MAP: Optional[Dict[str, str]] = None
async def get_deal_type_map() -> Dict[str, str]:
    global _DEAL_TYPE_MAP
    if _DEAL_TYPE_MAP is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_TYPE"})
        _DEAL_TYPE_MAP = {i["STATUS_ID"]: i["NAME"] for i in items}
        log.info("[cache] DEAL_TYPE: %s", len(_DEAL_TYPE_MAP))
    return _DEAL_TYPE_MAP

_CAT_STAGES: Dict[int, List[Dict[str, Any]]] = {}
async def get_category_stages(cat_id: int) -> List[Dict[str, Any]]:
    if cat_id not in _CAT_STAGES:
        st = await b24("crm.dealcategory.stage.list", id=cat_id)
        _CAT_STAGES[cat_id] = st or []
        log.info("[cache] CAT%s stages: %s", cat_id, len(_CAT_STAGES[cat_id]))
    return _CAT_STAGES[cat_id]

# ------------------------ Time helpers -------------------
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc   = end_local.astimezone(timezone.utc)
    label = start_local.strftime("%d.%m.%Y")
    return label, start_utc.isoformat(), end_utc.isoformat()

# ------------------------ “Підключення” rules ------------
# Бригадні стадії (кат.20)
_BRIGADE_STAGE = {"UC_XF8O6V", "UC_0XLPCN", "UC_204CP3", "UC_TNEW3Z", "UC_RMBZ37"}

def _is_connection(type_id: str, type_map: Dict[str, str]) -> bool:
    name = (type_map.get(type_id or "", "") or "").strip().lower()
    return "підключ" in name or "подключ" in name

# ------------------------ CAT0 exact-day / think ----------
def _stage_code(cat_id: int, status_id: str) -> str:
    return f"C{cat_id}:{status_id}"

async def _resolve_cat0_stage_ids() -> Tuple[str, str]:
    """Повертає (C0:5, C0:DETAILS) з ENV або з довідника Bitrix."""
    exact_day = CAT0_EXACT_DAY_STAGE_ID
    think     = CAT0_THINK_STAGE_ID
    if exact_day and think:
        return exact_day, think

    stages = await get_category_stages(0)
    sid_exact = None
    sid_think = None
    for s in stages:
        sid = s.get("STATUS_ID", "")
        nm  = (s.get("NAME", "") or "").lower()
        if sid == "5" or "конкретний день" in nm:
            sid_exact = sid
        if sid == "DETAILS" or "дума" in nm:
            sid_think = sid
    # fallback до відомих значень
    sid_exact = sid_exact or "5"
    sid_think = sid_think or "DETAILS"
    return _stage_code(0, sid_exact), _stage_code(0, sid_think)

# ------------------------ Company summary -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm, to = _day_bounds(offset_days)
    type_map = await get_deal_type_map()

    # 1) Підключення — подали сьогодні (кат.20)
    created_cat20 = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={">=DATE_CREATE": frm, "<DATE_CREATE": to, "CATEGORY_ID": 20},
        select=["ID", "TYPE_ID"],
    )
    conn_created = sum(1 for d in created_cat20 if _is_connection(d.get("TYPE_ID"), type_map))

    # 2) Підключення — закрили сьогодні (кат.20 → WON)
    closed_cat20 = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter={"STAGE_ID": "C20:WON", ">=DATE_MODIFY": frm, "<DATE_MODIFY": to},
        select=["ID", "TYPE_ID"],
    )
    conn_closed = sum(1 for d in closed_cat20 if _is_connection(d.get("TYPE_ID"), type_map))

    # 3) Підключення — активні на бригадах (кат.20, відкриті, стадія ∈ бригадні)
    brigade_stage_ids = {f"C20:{x}" for x in _BRIGADE_STAGE}
    open_cat20 = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={"CLOSED": "N", "CATEGORY_ID": 20},
        select=["ID", "TYPE_ID", "STAGE_ID"],
    )
    conn_active = sum(
        1 for d in open_cat20
        if (d.get("STAGE_ID") in brigade_stage_ids and _is_connection(d.get("TYPE_ID"), type_map))
    )

    # 4) Категорія 0 — «На конкретний день» та «Думають» (поточні відкриті)
    c0_exact_stage, c0_think_stage = await _resolve_cat0_stage_ids()

    # «На конкретний день»
    cat0_exact = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={"CLOSED": "N", "CATEGORY_ID": 0, "STAGE_ID": c0_exact_stage},
        select=["ID"],
    )
    # «Думають»
    cat0_think = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={"CLOSED": "N", "CATEGORY_ID": 0, "STAGE_ID": c0_think_stage},
        select=["ID"],
    )

    return {
        "date_label": label,
        "connections": {
            "created": conn_created,
            "closed":  conn_closed,
            "active":  conn_active,
        },
        "cat0": {
            "exact_day": len(cat0_exact),
            "think":     len(cat0_think),
        },
    }

def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c  = d["connections"]
    k0 = d["cat0"]
    lines = [
        f"🗓 <b>Дата: {dl}</b>",
        "",
        "📌 <b>Підключення</b>",
        f"Всього подали (кат.20) — <b>{c['created']}</b>",
        f"Закрили сьогодні (кат.20) — <b>{c['closed']}</b>",
        f"Активних на бригадах (кат.20) — <b>{c['active']}</b>",
        "",
        f"На конкретний день (кат.0) — <b>{k0['exact_day']}</b>",
        f"Думають (кат.0) — <b>{k0['think']}</b>",
    ]
    return "\n".join(lines)

# ------------------------ Utilities -----------------------
def _pad(s: str, n: int) -> str:
    s = str(s or ""); return s + " " * max(0, n - len(s))

async def render_category_stages_table(cat_id: int) -> str:
    stages = await get_category_stages(cat_id)
    rows = [(s.get("STATUS_ID", ""), s.get("NAME", "")) for s in stages]
    w_id = max([len("STATUS_ID")] + [len(x[0]) for x in rows]) + 2
    w_nm = max([len("NAME")] + [len(x[1]) for x in rows]) + 2
    out = []
    out.append(f"Категорія: {cat_id}\n")
    out.append("<code>")
    out.append(_pad("STATUS_ID", w_id) + _pad("NAME", w_nm))
    out.append(_pad("-" * len("STATUS_ID"), w_id) + _pad("-" * len("NAME"), w_nm))
    for sid, name in rows: out.append(_pad(sid, w_id) + _pad(name, w_nm))
    out.append("</code>")
    return "\n".join(out)

def _resolve_summary_chat() -> Optional[int]:
    if REPORT_SUMMARY_CHAT: return REPORT_SUMMARY_CHAT
    if "all" in REPORT_CHATS: return int(REPORT_CHATS["all"])
    return None

async def _safe_send(chat_id: int, text: str):
    for attempt in range(5):
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
            return
        except Exception as e:
            log.warning("telegram send failed: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    log.error("telegram send failed permanently")

async def send_company_summary(offset_days: int = 0) -> None:
    chat_id = _resolve_summary_chat()
    if not chat_id:
        log.warning("No REPORT_SUMMARY_CHAT nor REPORT_CHATS['all'] configured")
        return
    try:
        data = await build_company_summary(offset_days)
        await _safe_send(chat_id, format_company_summary(data))
    except Exception:
        log.exception("company summary failed")
        await _safe_send(chat_id, "❗️Помилка формування сумарного звіту")

# ------------------------ Commands -----------------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    # /report_now         — за сьогодні
    # /report_now 1       — за вчора
    parts = (m.text or "").split()
    try:
        offset = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        offset = 0
    await m.answer("🔄 Формую сумарний звіт…")
    await send_company_summary(offset)
    await m.answer("✅ Готово")

@dp.message(Command("cat_stages"))
async def cmd_cat_stages(m: Message):
    parts = (m.text or "").split()
    try:
        cat_id = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        cat_id = 0
    await m.answer("🔎 Збираю стадії категорії…")
    try:
        text = await render_category_stages_table(cat_id)
        await m.answer(text)
    except Exception as e:
        log.exception("cat_stages failed")
        await m.answer(f"❗️Помилка завантаження стадій: {html.escape(str(e))}")

# ------------------------ Scheduler ----------------------
def _next_run_dt(now_utc: datetime) -> datetime:
    hh, mm = map(int, REPORT_TIME.split(":", 1))
    now_local = now_utc.astimezone(REPORT_TZ)
    target_local = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target_local <= now_local: target_local += timedelta(days=1)
    return target_local.astimezone(timezone.utc)

async def scheduler_loop():
    log.info("[scheduler] started")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            nxt = _next_run_dt(now_utc)
            sleep_sec = max(1, (nxt - now_utc).total_seconds())
            log.info("[scheduler] next run at %s in %ss", nxt.isoformat(), int(sleep_sec))
            await asyncio.sleep(sleep_sec)
            log.info("[scheduler] tick -> sending summary")
            await send_company_summary(0)
        except Exception:
            log.exception("[scheduler] loop error")
            await asyncio.sleep(5)

# ------------------------ Webhook plumbing ---------------
@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    await bot.set_my_commands([
        BotCommand(command="report_now", description="Ручний запуск сумарного звіту (/report_now [offset])"),
        BotCommand(command="cat_stages", description="Показати стадії категорії (/cat_stages [id])"),
    ])
    url = f"{WEBHOOK_BASE}/webhook/{WEBHOOK_SECRET}"
    await bot.set_webhook(url)
    asyncio.create_task(scheduler_loop())
    log.info("[startup] webhook set to %s", url)

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    await HTTP.close()
    await bot.session.close()

@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        return {"ok": False}
    update = Update.model_validate(await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}
