# main.py — reports-bot
import asyncio
import html
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
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

# TZ та час щоденного звіту
REPORT_TZ_NAME = os.environ.get("REPORT_TZ", "Europe/Kyiv")
REPORT_TZ = ZoneInfo(REPORT_TZ_NAME)
REPORT_TIME = os.environ.get("REPORT_TIME", "19:00")  # HH:MM

# Куди слати: JSON-словник або один chat_id для всіх
# приклади:
#   REPORT_CHATS='{"1": -100123, "2": -100124, "3": -100125, "4": -100126, "5": -100127}'
#   REPORT_CHATS='{"all": -1001234567890}'
_raw_report_chats = os.environ.get("REPORT_CHATS", "")
if _raw_report_chats.strip():
    REPORT_CHATS: Dict[str, int] = json.loads(_raw_report_chats)
else:
    REPORT_CHATS = {}

# Якщо хочеш у звіті давати лінки на угоди:
B24_DOMAIN = os.environ.get("B24_DOMAIN", "").strip()

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
    delay = min(cap, base * (2 ** attempt))
    await asyncio.sleep(delay)

async def b24(method: str, **params) -> Any:
    url = f"{BITRIX_WEBHOOK_BASE}/{method}.json"
    # простий ретрай на ліміти/мережеві збої
    for attempt in range(6):
        try:
            async with HTTP.post(url, json=params) as resp:
                data = await resp.json()
                if "error" in data:
                    err = data["error"]
                    desc = data.get("error_description")
                    # ретраїмо тільки ліміт / тимчасові
                    if err in ("QUERY_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS"):
                        log.warning("Bitrix rate-limit: %s (%s), retry #%s", err, desc, attempt+1)
                        await _sleep_backoff(attempt)
                        continue
                    raise RuntimeError(f"B24 error: {err}: {desc}")
                return data.get("result")
        except aiohttp.ClientError as e:
            log.warning("Bitrix network error: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    raise RuntimeError("Bitrix request failed after retries")

async def b24_list(method: str, *, page_size: int = 200, throttle: float = 0.2, **params) -> List[Dict[str, Any]]:
    start = 0
    out: List[Dict[str, Any]] = []
    while True:
        payload = dict(params)
        payload["start"] = start
        res = await b24(method, **payload)
        chunk = res or []
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk.get("items", [])
        out.extend(chunk)
        if len(chunk) < page_size:
            break
        start += page_size
        if throttle:
            await asyncio.sleep(throttle)
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

# ------------------------ Classification ------------------
def normalize_type(type_name: str) -> str:
    t = (type_name or "").strip().lower()
    mapping_exact = {
        "підключення": "connection", "подключение": "connection",
        "ремонт": "repair",
        "сервісні роботи": "service", "сервисные работы": "service", "сервіс": "service", "сервис": "service",
        "перепідключення": "reconnection", "переподключение": "reconnection",
        "аварія": "accident", "авария": "accident",
        "будівництво": "construction", "строительство": "construction",
        "роботи по лінії": "linework", "работы по линии": "linework",
        "звернення в кц": "cc_request", "обращение в кц": "cc_request",
        "не выбран": "other", "не вибрано": "other", "інше": "other", "прочее": "other",
    }
    if t in mapping_exact:
        return mapping_exact[t]
    if any(k in t for k in ("підключ", "подключ")): return "connection"
    if "ремонт" in t: return "repair"
    if any(k in t for k in ("сервіс", "сервис")): return "service"
    if any(k in t for k in ("перепідключ", "переподключ")): return "reconnection"
    if "авар" in t: return "accident"
    if any(k in t for k in ("будівниц", "строит")): return "construction"
    if any(k in t for k in ("ліні", "линии")): return "linework"
    if any(k in t for k in ("кц", "контакт-центр", "колл-центр", "call")): return "cc_request"
    return "other"

REPORT_BUCKETS = [
    ("connection", "Підключення"),
    ("reconnection", "Перепідключення"),
    ("repair", "Ремонти"),
    ("service", "Сервісні роботи"),
    ("accident", "Аварії"),
    ("construction", "Будівництво"),
    ("linework", "Роботи по лінії"),
    ("cc_request", "Звернення в КЦ"),
    ("other", "Інше"),
]

# ------------------------ Brigade mapping -----------------
_BRIGADE_STAGE = {1: "UC_XF8O6V", 2: "UC_0XLPCN", 3: "UC_204CP3", 4: "UC_TNEW3Z", 5: "UC_RMBZ37"}
_BRIGADE_EXEC_OPTION_ID = {1: 5494, 2: 5496, 3: 5498, 4: 5500, 5: 5502}

# ------------------------ Time helpers -------------------
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    # межі доби за Києвом, конвертовані в UTC ISO
    now_kyiv = datetime.now(REPORT_TZ)
    start_kyiv = (now_kyiv - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_kyiv = start_kyiv + timedelta(days=1)
    start_utc = start_kyiv.astimezone(timezone.utc)
    end_utc = end_kyiv.astimezone(timezone.utc)
    label = start_kyiv.strftime("%d.%m.%Y")
    return label, start_utc.isoformat(), end_utc.isoformat()

# ------------------------ Report core --------------------
async def build_daily_report(brigade: int, offset_days: int) -> Tuple[str, Dict[str, int], int]:
    label, frm, to = _day_bounds(offset_days)
    deal_type_map = await get_deal_type_map()

    exec_opt = _BRIGADE_EXEC_OPTION_ID.get(brigade)
    filter_closed = {"STAGE_ID": "C20:WON", ">=DATE_MODIFY": frm, "<DATE_MODIFY": to}
    if exec_opt:
        filter_closed["UF_CRM_1611995532420"] = exec_opt

    closed = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter=filter_closed,
        select=["ID", "TYPE_ID"],
        page_size=200,
    )

    counts = {k: 0 for k, _ in REPORT_BUCKETS}
    for d in closed:
        tcode = d.get("TYPE_ID") or ""
        tname = deal_type_map.get(tcode, tcode)
        cls = normalize_type(tname)
        counts[cls] = counts.get(cls, 0) + 1

    stage_code = _BRIGADE_STAGE[brigade]
    active = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={"CLOSED": "N", "STAGE_ID": f"C20:{stage_code}"},
        select=["ID"],
        page_size=200,
    )
    return label, counts, len(active)

def format_report(brigade: int, date_label: str, counts: Dict[str, int], active_left: int) -> str:
    total = sum(counts.values())
    lines = [f"<b>Бригада №{brigade} — {date_label}</b>", "", f"<b>Закрито задач:</b> {total}", ""]
    for key, title in REPORT_BUCKETS:
        lines.append(f"{title} — {counts.get(key, 0)}")
    lines += ["", f"<b>Активних задач на бригаді залишилось:</b> {active_left}"]
    return "\n".join(lines)

async def _safe_send(chat_id: int, text: str):
    # ретрай на Telegram timeouts
    for attempt in range(5):
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
            return
        except Exception as e:
            log.warning("telegram send failed: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    log.error("telegram send failed permanently")

async def _send_one_brigade_report(brigade: int, chat_id: int, offset_days: int) -> None:
    try:
        label, counts, active_left = await build_daily_report(brigade, offset_days)
        await _safe_send(chat_id, format_report(brigade, label, counts, active_left))
    except Exception as e:
        log.exception("Report for brigade %s failed", brigade)
        await _safe_send(chat_id, f"❗️Помилка формування звіту для бригади №{brigade}: {html.escape(str(e))}")

def _resolve_chat_for_brigade(b: int) -> Optional[int]:
    # спершу точна бригада "1".."5", далі "all"
    if str(b) in REPORT_CHATS:
        return int(REPORT_CHATS[str(b)])
    if b in REPORT_CHATS:  # якщо раптом передали як int у JSON
        return int(REPORT_CHATS[b])
    if "all" in REPORT_CHATS:
        return int(REPORT_CHATS["all"])
    return None

async def send_all_brigades_report(offset_days: int = 0) -> None:
    tasks = []
    for b in (1, 2, 3, 4, 5):
        chat_id = _resolve_chat_for_brigade(b)
        if not chat_id:
            log.warning("No chat configured for brigade %s", b)
            continue
        tasks.append(_send_one_brigade_report(b, chat_id, offset_days))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# ------------------------ Manual command -----------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    try:
        parts = (m.text or "").split()
        offset = int(parts[1]) if len(parts) > 1 else 0
    except:
        offset = 0
    await m.answer("Генерую звіти…")
    await send_all_brigades_report(offset)
    await m.answer("Готово ✅")

# ------------------------ Scheduler ----------------------
def _next_run_dt(now_utc: datetime) -> datetime:
    """Обчислити найближчу дату/час запуску 19:00 за REPORT_TZ, повернути у UTC."""
    hh, mm = map(int, REPORT_TIME.split(":", 1))
    now_local = now_utc.astimezone(REPORT_TZ)
    target_local = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target_local <= now_local:
        target_local = target_local + timedelta(days=1)
    return target_local.astimezone(timezone.utc)

async def scheduler_loop():
    log.info("[scheduler] started")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            nxt = _next_run_dt(now_utc)
            sleep_sec = (nxt - now_utc).total_seconds()
            log.info("[scheduler] next run at %s (%s sec)", nxt.isoformat(), int(sleep_sec))
            await asyncio.sleep(max(1, sleep_sec))
            log.info("[scheduler] tick -> sending daily reports")
            await send_all_brigades_report(0)
        except Exception:
            log.exception("[scheduler] loop error")
            await asyncio.sleep(5)

# ------------------------ Webhook plumbing ---------------
@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    await bot.set_my_commands([
        BotCommand(command="report_now", description="Ручний запуск звітів (/report_now [offset])"),
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
