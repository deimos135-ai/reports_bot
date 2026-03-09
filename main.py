# main.py — reports-bot (FINAL VERSION)
import asyncio
import html
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import BotCommand, Message, Update
from aiogram.exceptions import TelegramRetryAfter
from zoneinfo import ZoneInfo

# ------------------------ Settings ------------------------

BOT_TOKEN = os.environ["BOT_TOKEN"]
BITRIX_WEBHOOK_BASE = os.environ["BITRIX_WEBHOOK_BASE"].rstrip("/")
WEBHOOK_BASE = os.environ["WEBHOOK_BASE"].rstrip("/")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "secret")

REPORT_TZ = ZoneInfo(os.environ.get("REPORT_TZ", "Europe/Kyiv"))
REPORT_TIME = os.environ.get("REPORT_TIME", "19:00")

SCHEDULER_ENABLED = os.environ.get("SCHEDULER_ENABLED", "true").lower() in ("1", "true", "yes")
LEADER = os.environ.get("LEADER", "0") == "1"

# ------------------------ Telegram targets ------------------------

_raw_report_chats = os.environ.get("REPORT_CHATS", "")

def _normalize_report_chats(raw: str) -> Dict[str, Any]:
    if not raw.strip():
        return {}

    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = raw.strip()

    if isinstance(parsed, int):
        return {"all": {"chat_id": parsed, "thread_id": None}}

    if isinstance(parsed, str) and parsed.lstrip("-").isdigit():
        return {"all": {"chat_id": int(parsed), "thread_id": None}}

    if isinstance(parsed, dict):
        out = {}
        for k, v in parsed.items():
            if isinstance(v, dict):
                out[str(k)] = {
                    "chat_id": int(v["chat_id"]),
                    "thread_id": v.get("thread_id"),
                }
        return out

    return {}

REPORT_CHATS = _normalize_report_chats(_raw_report_chats)

# ------------------------ Logging ------------------------

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("report_bot")

# ------------------------ App/Bot ------------------------

app = FastAPI()

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()
HTTP: aiohttp.ClientSession

# ------------------------ Brigade mapping ------------------------

_BRIGADE_STAGE = {
    1: "UC_XF8O6V",
    2: "UC_0XLPCN",
    3: "UC_204CP3",
    4: "UC_TNEW3Z",
    5: "UC_RMBZ37",
    6: "UC_CY6YW8",
    7: "7",
}

_BRIGADE_EXEC_OPTION_ID = {
    1: 5494,
    2: 5496,
    3: 5498,
    4: 5500,
    5: 5502,
    6: 5674,
    7: 5678,
}

_BRIGADE_TITLE = {
    1: "Бригада №1",
    2: "Бригада №2",
    3: "Бригада №3",
    4: "Бригада №4",
    5: "Бригада №5",
    6: "Бригада №6",
    7: "Інші бригади",
}

BRIGADES = (1, 2, 3, 4, 5, 6, 7)

# ------------------------ Categories ------------------------

REPORT_BUCKETS = [
    ("connection", "🔌 Підключення"),
    ("reconnection", "♻️ Перепідключення"),
    ("repair", "🛠 Ремонти"),
    ("service", "⚙️ Сервісні роботи"),
    ("accident", "🚨 Аварії"),
    ("linework", "📡 Роботи по лінії"),
    ("other", "📂 Інше"),
]

# ------------------------ Helpers ------------------------

def normalize_type(type_name: str) -> str:
    t = (type_name or "").lower()

    if "підключ" in t:
        return "connection"

    if "ремонт" in t:
        return "repair"

    if "сервіс" in t:
        return "service"

    if "перепідключ" in t:
        return "reconnection"

    if "авар" in t:
        return "accident"

    if "ліні" in t:
        return "linework"

    return "other"

# ------------------------ Bitrix helpers ------------------------

async def b24(method: str, **params):

    url = f"{BITRIX_WEBHOOK_BASE}/{method}.json"

    async with HTTP.post(url, json=params) as resp:
        data = await resp.json()

        if "error" in data:
            raise RuntimeError(data)

        return data.get("result")

async def b24_list(method: str, **params):

    start = 0
    result = []

    while True:

        res = await b24(method, start=start, **params)

        result.extend(res)

        if len(res) < 200:
            break

        start += 200

    return result

# ------------------------ Time helpers ------------------------

def _day_bounds(offset_days: int = 0):

    now = datetime.now(REPORT_TZ)

    start = (now - timedelta(days=offset_days)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    end = start + timedelta(days=1)

    return (
        start.strftime("%d.%m.%Y"),
        start.astimezone(timezone.utc).isoformat(),
        end.astimezone(timezone.utc).isoformat(),
    )

# ------------------------ Report logic ------------------------

async def build_daily_report(brigade: int, offset_days: int):

    label, frm, to = _day_bounds(offset_days)

    exec_id = _BRIGADE_EXEC_OPTION_ID.get(brigade)

    filter_closed = {
        "STAGE_ID": "C20:WON",
        ">=DATE_MODIFY": frm,
        "<DATE_MODIFY": to,
    }

    if exec_id:
        filter_closed["UF_CRM_1611995532420"] = exec_id

    closed = await b24_list(
        "crm.deal.list",
        filter=filter_closed,
        select=["ID", "TYPE_ID"]
    )

    closed_counts = {k: 0 for k, _ in REPORT_BUCKETS}

    for d in closed:

        cls = normalize_type(d.get("TYPE_ID"))

        closed_counts[cls] += 1

    stage_code = _BRIGADE_STAGE[brigade]

    active = await b24_list(
        "crm.deal.list",
        filter={
            "CLOSED": "N",
            "STAGE_ID": f"C20:{stage_code}"
        },
        select=["ID", "TYPE_ID", "DATE_CREATE"]
    )

    active_counts = {k: 0 for k, _ in REPORT_BUCKETS}

    overdue = 0

    now = datetime.now(timezone.utc)

    for d in active:

        cls = normalize_type(d.get("TYPE_ID"))

        active_counts[cls] += 1

        if cls == "repair":

            created = datetime.fromisoformat(
                d["DATE_CREATE"].replace("Z", "+00:00")
            )

            if now - created > timedelta(hours=24):
                overdue += 1

    return label, closed_counts, active_counts, overdue

# ------------------------ Telegram send ------------------------

_last_send: Dict[int, float] = {}

async def safe_send(chat_id: int, text: str, thread_id: Optional[int]):

    now = time.time()

    if chat_id in _last_send:
        gap = 3 - (now - _last_send[chat_id])

        if gap > 0:
            await asyncio.sleep(gap)

    for _ in range(3):

        try:

            await bot.send_message(
                chat_id,
                text,
                message_thread_id=thread_id,
                disable_web_page_preview=True,
            )

            _last_send[chat_id] = time.time()

            return

        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)

# ------------------------ Format report ------------------------

def format_report(brigade, label, closed_counts, active_counts, overdue):

    closed_total = sum(closed_counts.values())
    active_total = sum(active_counts.values())

    title = _BRIGADE_TITLE.get(brigade)

    lines = [
        f"📝 <b>Звіт: {title} — {label}</b>",
        "",
        f"✅ <b>Закрито задач:</b> {closed_total}",
    ]

    for k, t in REPORT_BUCKETS:
        lines.append(f"{t}: {closed_counts[k]}")

    lines += [
        "",
        f"📊 <b>Активні задачі:</b> {active_total}",
    ]

    for k, t in REPORT_BUCKETS:
        lines.append(f"{t}: {active_counts[k]}")

    lines.append("")
    lines.append(f"⚠️ <b>Ремонтів >24h:</b> {overdue}")

    return "\n".join(lines)

# ------------------------ Resolve chat ------------------------

def resolve_target(brigade):

    raw = REPORT_CHATS.get(str(brigade)) or REPORT_CHATS.get("all")

    if not raw:
        return None

    return raw["chat_id"], raw.get("thread_id")

# ------------------------ Send reports ------------------------

async def send_all_brigades_report(offset_days=0):

    tasks = []

    for b in BRIGADES:

        target = resolve_target(b)

        if not target:
            continue

        chat_id, thread_id = target

        label, closed_counts, active_counts, overdue = await build_daily_report(
            b,
            offset_days
        )

        text = format_report(b, label, closed_counts, active_counts, overdue)

        tasks.append(safe_send(chat_id, text, thread_id))

    await asyncio.gather(*tasks)

# ------------------------ Commands ------------------------

@dp.message(Command("report_now"))
async def report_now(m: Message):

    await m.answer("Генерую звіти…")

    await send_all_brigades_report()

    await m.answer("Готово")

@dp.message(Command("where_am_i"))
async def where_am_i(m: Message):

    await m.answer(
        f"chat_id=<code>{m.chat.id}</code>\n"
        f"thread_id=<code>{getattr(m,'message_thread_id',None)}</code>"
    )

# ------------------------ Scheduler ------------------------

def next_run():

    hh, mm = map(int, REPORT_TIME.split(":"))

    now = datetime.now(REPORT_TZ)

    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)

    if target <= now:
        target += timedelta(days=1)

    return target.astimezone(timezone.utc)

async def scheduler():

    while True:

        now = datetime.now(timezone.utc)

        nxt = next_run()

        await asyncio.sleep((nxt - now).total_seconds())

        await send_all_brigades_report()

# ------------------------ Webhook ------------------------

@app.on_event("startup")
async def startup():

    global HTTP

    HTTP = aiohttp.ClientSession()

    await bot.set_my_commands([
        BotCommand(command="report_now", description="Запустити звіт"),
        BotCommand(command="where_am_i", description="Показати chat_id"),
    ])

    await bot.set_webhook(f"{WEBHOOK_BASE}/webhook/{WEBHOOK_SECRET}")

    if SCHEDULER_ENABLED and LEADER:
        asyncio.create_task(scheduler())

@app.on_event("shutdown")
async def shutdown():

    await bot.delete_webhook()

    await HTTP.close()

    await bot.session.close()

@app.post("/webhook/{secret}")
async def webhook(secret: str, request: Request):

    if secret != WEBHOOK_SECRET:
        return {"ok": False}

    update = Update.model_validate(await request.json())

    await dp.feed_update(bot, update)

    return {"ok": True}
