# report_bot/main.py
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

# ------------------------ Settings ------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
BITRIX_WEBHOOK_BASE = os.environ["BITRIX_WEBHOOK_BASE"].rstrip("/")
WEBHOOK_BASE = os.environ["WEBHOOK_BASE"].rstrip("/")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "secret")
REPORT_TZ = os.environ.get("REPORT_TZ", "Europe/Kyiv")
REPORT_TIME = os.environ.get("REPORT_TIME", "19:00")
REPORT_CHATS = json.loads(os.environ.get("REPORT_CHATS", "{}"))  # {"1": chat_id, ...}

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

# ------------------------ Bitrix helpers ------------------
async def b24(method: str, **params) -> Any:
    url = f"{BITRIX_WEBHOOK_BASE}/{method}.json"
    async with HTTP.post(url, json=params) as resp:
        data = await resp.json()
        if "error" in data:
            raise RuntimeError(f"B24 error: {data['error']}: {data.get('error_description')}")
        return data.get("result")

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
# стадії колонок бригад у воронці C20
_BRIGADE_STAGE = {1: "UC_XF8O6V", 2: "UC_0XLPCN", 3: "UC_204CP3", 4: "UC_TNEW3Z", 5: "UC_RMBZ37"}
# виконавець (multi) — UF_CRM_1611995532420: option_id
_BRIGADE_EXEC_OPTION_ID = {1: 5494, 2: 5496, 3: 5498, 4: 5500, 5: 5502}

# ------------------------ Time helpers -------------------
def _tzinfo() -> timezone:
    # простий мапінг, щоб не тягнути pytz — працюємо в UTC + ручний офсет
    # для Europe/Kyiv (UTC+2/+3) краще просто працювати від UTC півночі/19:00 через локальні межі:
    return timezone.utc

def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    start = (now + timedelta(days=-offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    label = start.strftime("%d.%m.%Y")
    return label, start.isoformat(), end.isoformat()

# ------------------------ Report core --------------------
async def build_daily_report(brigade: int, offset_days: int) -> Tuple[str, Dict[str, int], int]:
    label, frm, to = _day_bounds(offset_days)
    deal_type_map = await get_deal_type_map()

    # Закриті за добу цією бригадою (успішні)
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

    # Скільки ще висить у колонці бригади
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

async def send_all_brigades_report(offset_days: int = 0) -> None:
    tasks = []
    for b in (1, 2, 3, 4, 5):
        chat_id = REPORT_CHATS.get(str(b)) or REPORT_CHATS.get(b) or REPORT_CHATS.get("all")
        if not chat_id:
            log.warning("No chat configured for brigade %s", b)
            continue
        tasks.append(_send_one_brigade_report(b, chat_id, offset_days))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def _send_one_brigade_report(brigade: int, chat_id: int, offset_days: int) -> None:
    try:
        label, counts, active_left = await build_daily_report(brigade, offset_days)
        text = format_report(brigade, label, counts, active_left)
        await bot.send_message(chat_id, text, disable_web_page_preview=True)
    except Exception as e:
        log.exception("Report for brigade %s failed", brigade)
        await bot.send_message(chat_id, f"❗️Помилка формування звіту для бригади №{brigade}: {html.escape(str(e))}")

# ------------------------ Manual command -----------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    # /report_now або /report_now 1 (вчора)
    try:
        parts = (m.text or "").split()
        offset = int(parts[1]) if len(parts) > 1 else 0
    except:
        offset = 0
    await m.answer("Генерую звіти…")
    await send_all_brigades_report(offset)
    await m.answer("Готово ✅")

# ------------------------ Scheduler ----------------------
async def _sleep_until_19_local():
    # обчислюємо від UTC — просто тримаємося години REPORT_TIME у локальній зоні
    hh, mm = map(int, REPORT_TIME.split(":", 1))
    # Вирахуємо найближчий час запуску в локальній зоні через naive підхід:
    # Беремо теперішній UTC і припускаємо, що 19:00 Київ ~ 16:00/17:00 UTC (DST).
    # Щоб не ускладнювати: прокидаємось кожні 60 секунд і перевіряємо локальний час.
    while True:
        now = datetime.now()
        # просте порівняння локального часу контейнера (налаштуй TZ=Europe/Kyiv у контейнері)
        if now.hour == hh and now.minute == mm:
            return
        await asyncio.sleep(30)

async def scheduler_loop():
    log.info("[scheduler] started")
    while True:
        try:
            await _sleep_until_19_local()
            log.info("[scheduler] 19:00 tick -> sending daily reports")
            await send_all_brigades_report(0)
            # щоб не двоїтись в цю ж хвилину
            await asyncio.sleep(65)
        except Exception:
            log.exception("[scheduler] loop error")
            await asyncio.sleep(5)

# ------------------------ Webhook plumbing ---------------
@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    await bot.set_my_commands([
        BotCommand(command="report_now", description="Ручний запуск звітів"),
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
