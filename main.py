# main.py — reports-bot (stable summary, no external AI)
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

REPORT_TZ_NAME = os.environ.get("REPORT_TZ", "Europe/Kyiv")
REPORT_TZ = ZoneInfo(REPORT_TZ_NAME)
REPORT_TIME = os.environ.get("REPORT_TIME", "19:00")

SCHEDULER_ENABLED = os.environ.get("SCHEDULER_ENABLED", "true").lower() in ("1", "true", "yes", "y")
LEADER = os.environ.get("LEADER", "0") == "1"

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
        normalized: Dict[str, Any] = {}
        for k, v in parsed.items():
            key = str(k)

            if isinstance(v, int):
                normalized[key] = {"chat_id": int(v), "thread_id": None}
                continue

            if isinstance(v, str) and v.lstrip("-").isdigit():
                normalized[key] = {"chat_id": int(v), "thread_id": None}
                continue

            if isinstance(v, dict):
                chat_id = v.get("chat_id")
                thread_id = v.get("thread_id")
                if chat_id is None:
                    continue
                try:
                    normalized[key] = {
                        "chat_id": int(chat_id),
                        "thread_id": int(thread_id) if thread_id is not None else None,
                    }
                except Exception:
                    continue

        return normalized

    return {}


REPORT_CHATS: Dict[str, Any] = _normalize_report_chats(_raw_report_chats)

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
    for attempt in range(6):
        try:
            async with HTTP.post(url, json=params) as resp:
                data = await resp.json()
                if "error" in data:
                    err = data["error"]
                    desc = data.get("error_description")
                    if err in ("QUERY_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS"):
                        log.warning("Bitrix rate-limit: %s (%s), retry #%s", err, desc, attempt + 1)
                        await _sleep_backoff(attempt)
                        continue
                    raise RuntimeError(f"B24 error: {err}: {desc}")
                return data.get("result")
        except aiohttp.ClientError as e:
            log.warning("Bitrix network error: %s, retry #%s", e, attempt + 1)
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
        "підключення": "connection",
        "подключение": "connection",
        "ремонт": "repair",
        "сервісні роботи": "service",
        "сервисные работы": "service",
        "сервіс": "service",
        "сервис": "service",
        "перепідключення": "reconnection",
        "переподключение": "reconnection",
        "аварія": "accident",
        "авария": "accident",
        "роботи по лінії": "linework",
        "работы по линии": "linework",
        "не выбран": "other",
        "не вибрано": "other",
        "інше": "other",
        "прочее": "other",
    }

    if t in mapping_exact:
        return mapping_exact[t]

    if any(k in t for k in ("підключ", "подключ")):
        return "connection"
    if "ремонт" in t:
        return "repair"
    if any(k in t for k in ("сервіс", "сервис")):
        return "service"
    if any(k in t for k in ("перепідключ", "переподключ")):
        return "reconnection"
    if "авар" in t:
        return "accident"
    if any(k in t for k in ("ліні", "линии")):
        return "linework"

    return "other"


REPORT_BUCKETS = [
    ("connection", "🔌 Підключення"),
    ("reconnection", "♻️ Перепідключення"),
    ("repair", "🛠 Ремонти"),
    ("service", "⚙️ Сервісні роботи"),
    ("accident", "🚨 Аварії"),
    ("linework", "📡 Роботи по лінії"),
    ("other", "📂 Інше"),
]

# ------------------------ Brigade mapping -----------------
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

# ------------------------ Time helpers --------------------
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    now_kyiv = datetime.now(REPORT_TZ)
    start_kyiv = (now_kyiv - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_kyiv = start_kyiv + timedelta(days=1)

    start_utc = start_kyiv.astimezone(timezone.utc)
    end_utc = end_kyiv.astimezone(timezone.utc)

    label = start_kyiv.strftime("%d.%m.%Y")
    return label, start_utc.isoformat(), end_utc.isoformat()


def _day_key_in_tz(offset_days: int = 0) -> str:
    return (datetime.now(REPORT_TZ) - timedelta(days=offset_days)).strftime("%Y-%m-%d")


def _is_evening_report() -> bool:
    try:
        hh, _ = map(int, REPORT_TIME.split(":", 1))
        return hh >= 18
    except Exception:
        return False

# ------------------------ Bitrix datetime parser ----------
def _parse_b24_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None

    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            return None

# ------------------------ Anti-duplicate / rate limiting --
_sent_guard: Dict[Tuple[int, int, str, int], bool] = {}
_last_target_send_ts: Dict[Tuple[int, int], float] = {}
_CHAT_MIN_INTERVAL_SEC = 5


def _thread_key(thread_id: Optional[int]) -> int:
    return thread_id or 0

# ------------------------ Summary helpers -----------------
def _empty_bucket_counts() -> Dict[str, int]:
    return {k: 0 for k, _ in REPORT_BUCKETS}


def _sum_bucket_counts(items: List[Dict[str, int]]) -> Dict[str, int]:
    total = _empty_bucket_counts()
    for item in items:
        for k in total.keys():
            total[k] += int(item.get(k, 0))
    return total


def build_team_summary(report_rows: List[Dict[str, Any]]) -> str:
    total_closed = _sum_bucket_counts([r["closed_counts"] for r in report_rows])
    total_active = _sum_bucket_counts([r["active_counts"] for r in report_rows])
    total_overdue = sum(int(r["overdue_repairs_24h"]) for r in report_rows)

    best_brigade = max(report_rows, key=lambda r: sum(r["closed_counts"].values()), default=None)
    best_connection = max(report_rows, key=lambda r: int(r["closed_counts"].get("connection", 0)), default=None)
    best_repair = max(report_rows, key=lambda r: int(r["closed_counts"].get("repair", 0)), default=None)
    most_overdue = max(report_rows, key=lambda r: int(r["overdue_repairs_24h"]), default=None)

    best_title = _BRIGADE_TITLE.get(best_brigade["brigade"], "—") if best_brigade else "—"
    conn_title = _BRIGADE_TITLE.get(best_connection["brigade"], "—") if best_connection else "—"
    repair_title = _BRIGADE_TITLE.get(best_repair["brigade"], "—") if best_repair else "—"

    closed_total = sum(total_closed.values())
    active_total = sum(total_active.values())

    lines = [
        "🤖 <b>Підсумок дня</b>",
        "",
        f"За сьогодні загалом виконано <b>{closed_total}</b> задач.",
        f"Найбільше виконано підключень — <b>{total_closed['connection']}</b>, ремонтів — <b>{total_closed['repair']}</b>, сервісних робіт — <b>{total_closed['service']}</b>.",
    ]

    if best_brigade and sum(best_brigade["closed_counts"].values()) > 0:
        lines.append(f"<b>Бригада дня — {best_title}</b>. Чудовий результат, так тримати 💪")

    if best_connection and int(best_connection["closed_counts"].get("connection", 0)) > 0:
        lines.append(
            f"Лідер по підключеннях — <b>{conn_title}</b> "
            f"({int(best_connection['closed_counts'].get('connection', 0))})."
        )

    if best_repair and int(best_repair["closed_counts"].get("repair", 0)) > 0:
        lines.append(
            f"Лідер по ремонтах — <b>{repair_title}</b> "
            f"({int(best_repair['closed_counts'].get('repair', 0))})."
        )

    lines.append(f"В активній роботі залишається <b>{active_total}</b> задач.")

    if total_overdue > 0 and most_overdue:
        overdue_title = _BRIGADE_TITLE.get(most_overdue["brigade"], "—")
        lines.append(
            f"Ремонтів понад 24 години — <b>{total_overdue}</b>. "
            f"Зона уваги на завтра, особливо по <b>{overdue_title}</b>."
        )
    else:
        lines.append("Прострочених ремонтів понад 24 години сьогодні немає — це хороший темп.")

    if _is_evening_report():
        lines.append("Гарного вечора команді 😊")

    return "\n".join(lines)


async def _send_summary_after_reports(report_rows: List[Dict[str, Any]]) -> None:
    if not report_rows:
        return

    try:
        first = report_rows[0]
        text = build_team_summary(report_rows)
        await _safe_send(
            first["chat_id"],
            text,
            thread_id=first["thread_id"],
        )
    except Exception:
        log.exception("Sending summary failed")

# ------------------------ Report core ---------------------
async def build_daily_report(brigade: int, offset_days: int) -> Tuple[str, Dict[str, int], Dict[str, int], int]:
    label, frm, to = _day_bounds(offset_days)
    deal_type_map = await get_deal_type_map()

    exec_opt = _BRIGADE_EXEC_OPTION_ID.get(brigade)
    filter_closed = {
        "STAGE_ID": "C20:WON",
        ">=DATE_MODIFY": frm,
        "<DATE_MODIFY": to,
    }
    if exec_opt:
        filter_closed["UF_CRM_1611995532420"] = exec_opt

    closed = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter=filter_closed,
        select=["ID", "TYPE_ID"],
        page_size=200,
    )

    closed_counts = {k: 0 for k, _ in REPORT_BUCKETS}
    for d in closed:
        tcode = d.get("TYPE_ID") or ""
        tname = deal_type_map.get(tcode, tcode)
        cls = normalize_type(tname)
        closed_counts[cls] = closed_counts.get(cls, 0) + 1

    stage_code = _BRIGADE_STAGE[brigade]
    active = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={"CLOSED": "N", "STAGE_ID": f"C20:{stage_code}"},
        select=["ID", "TYPE_ID", "DATE_CREATE"],
        page_size=200,
    )

    active_counts = {k: 0 for k, _ in REPORT_BUCKETS}
    overdue_repairs_24h = 0

    now_utc = datetime.now(timezone.utc)
    for d in active:
        tcode = d.get("TYPE_ID") or ""
        tname = deal_type_map.get(tcode, tcode)
        cls = normalize_type(tname)
        active_counts[cls] = active_counts.get(cls, 0) + 1

        if cls == "repair":
            created = _parse_b24_dt(d.get("DATE_CREATE"))
            if created:
                age = now_utc - created.astimezone(timezone.utc)
                if age > timedelta(hours=24):
                    overdue_repairs_24h += 1

    return label, closed_counts, active_counts, overdue_repairs_24h


def format_report(
    brigade: int,
    date_label: str,
    closed_counts: Dict[str, int],
    active_counts: Dict[str, int],
    overdue_repairs_24h: int,
) -> str:
    closed_total = sum(closed_counts.values())
    active_total = sum(active_counts.values())
    warn_emoji = "⚠️" if overdue_repairs_24h > 0 else "🕒"
    brigade_title = _BRIGADE_TITLE.get(brigade, f"Бригада №{brigade}")

    lines = [
        f"📝 <b>Звіт: {brigade_title} — {date_label}</b>",
        "",
        f"✅ <b>Закрито задач:</b> {closed_total}",
    ]
    for key, title in REPORT_BUCKETS:
        lines.append(f"{title}: {closed_counts.get(key, 0)}")

    lines += [
        "",
        f"📊 <b>Активні задачі (усього):</b> {active_total}",
    ]
    for key, title in REPORT_BUCKETS:
        lines.append(f"{title}: {active_counts.get(key, 0)}")

    lines += [
        "",
        f"{warn_emoji} <b>Ремонтів відкритих понад 24 години:</b> {overdue_repairs_24h}",
    ]

    return "\n".join(lines)

# ------------------------ Telegram target helpers ---------
def _resolve_target_for_brigade(b: int) -> Optional[Dict[str, Optional[int]]]:
    if not isinstance(REPORT_CHATS, dict):
        return None

    raw = REPORT_CHATS.get(str(b), REPORT_CHATS.get("all"))
    if not raw:
        return None

    if isinstance(raw, dict):
        chat_id = raw.get("chat_id")
        thread_id = raw.get("thread_id")
        if chat_id is None:
            return None
        try:
            return {
                "chat_id": int(chat_id),
                "thread_id": int(thread_id) if thread_id is not None else None,
            }
        except Exception:
            return None

    if isinstance(raw, int):
        return {"chat_id": int(raw), "thread_id": None}
    if isinstance(raw, str) and raw.lstrip("-").isdigit():
        return {"chat_id": int(raw), "thread_id": None}

    return None


async def _safe_send(chat_id: int, text: str, thread_id: Optional[int] = None):
    target_key = (chat_id, _thread_key(thread_id))

    now = time.time()
    last = _last_target_send_ts.get(target_key, 0.0)
    gap = _CHAT_MIN_INTERVAL_SEC - (now - last)
    if gap > 0:
        await asyncio.sleep(gap)

    for attempt in range(3):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                message_thread_id=thread_id,
                disable_web_page_preview=True,
            )
            _last_target_send_ts[target_key] = time.time()
            return
        except TelegramRetryAfter as e:
            retry_after = int(getattr(e, "retry_after", 15))
            log.warning(
                "telegram 429 in chat %s thread %s, retry_after=%s (attempt %s)",
                chat_id, thread_id, retry_after, attempt + 1
            )
            await asyncio.sleep(retry_after)
        except Exception as e:
            log.warning(
                "telegram send failed: chat=%s thread=%s err=%s retry #%s",
                chat_id, thread_id, e, attempt + 1
            )
            await asyncio.sleep(2 + attempt * 2)

    log.error("telegram send failed permanently (chat=%s thread=%s)", chat_id, thread_id)


async def _send_one_brigade_report(
    brigade: int,
    chat_id: int,
    thread_id: Optional[int],
    offset_days: int,
) -> Optional[Dict[str, Any]]:
    try:
        day_key = _day_key_in_tz(offset_days)
        guard_key = (chat_id, _thread_key(thread_id), day_key, brigade)

        if _sent_guard.get(guard_key):
            log.info(
                "Skip duplicate: chat=%s thread=%s day=%s brigade=%s",
                chat_id, thread_id, day_key, brigade
            )
            return None

        label, closed_counts, active_counts, overdue_repairs_24h = await build_daily_report(brigade, offset_days)
        await _safe_send(
            chat_id,
            format_report(brigade, label, closed_counts, active_counts, overdue_repairs_24h),
            thread_id=thread_id,
        )

        _sent_guard[guard_key] = True

        return {
            "brigade": brigade,
            "date_label": label,
            "closed_counts": closed_counts,
            "active_counts": active_counts,
            "overdue_repairs_24h": overdue_repairs_24h,
            "chat_id": chat_id,
            "thread_id": thread_id,
        }
    except Exception as e:
        log.exception("Report for brigade %s failed", brigade)
        await _safe_send(
            chat_id,
            f"❗️Помилка формування звіту для бригади №{brigade}: {html.escape(str(e))}",
            thread_id=thread_id,
        )
        return None


async def send_all_brigades_report(offset_days: int = 0) -> None:
    tasks = []

    for b in BRIGADES:
        target = _resolve_target_for_brigade(b)
        if not target:
            log.warning("No target configured for brigade %s", b)
            continue

        tasks.append(
            _send_one_brigade_report(
                brigade=b,
                chat_id=target["chat_id"],
                thread_id=target["thread_id"],
                offset_days=offset_days,
            )
        )

    if not tasks:
        return

    results = await asyncio.gather(*tasks, return_exceptions=True)

    report_rows: List[Dict[str, Any]] = []
    for r in results:
        if isinstance(r, dict):
            report_rows.append(r)

    if report_rows:
        asyncio.create_task(_send_summary_after_reports(report_rows))

# ------------------------ Manual commands -----------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    try:
        parts = (m.text or "").split()
        offset = int(parts[1]) if len(parts) > 1 else 0
    except Exception:
        offset = 0

    await m.answer("Генерую звіти… ⏳")
    await send_all_brigades_report(offset)
    await m.answer("Готово ✅")


@dp.message(Command("where_am_i"))
async def where_am_i(m: Message):
    await m.answer(
        "chat_id=<code>{}</code>\nthread_id=<code>{}</code>".format(
            m.chat.id,
            getattr(m, "message_thread_id", None),
        )
    )

# ------------------------ Scheduler -----------------------
def _next_run_dt(now_utc: datetime) -> datetime:
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
            valid_days = {
                _day_key_in_tz(0),
                _day_key_in_tz(1),
                _day_key_in_tz(2),
            }
            for k in list(_sent_guard.keys()):
                if k[2] not in valid_days:
                    _sent_guard.pop(k, None)

            now_utc = datetime.now(timezone.utc)
            nxt = _next_run_dt(now_utc)
            sleep_sec = (nxt - now_utc).total_seconds()
            if sleep_sec < 1:
                sleep_sec = 1

            log.info("[scheduler] next run at %s (%s sec)", nxt.isoformat(), int(sleep_sec))
            await asyncio.sleep(sleep_sec)

            log.info("[scheduler] tick -> sending daily reports")
            await send_all_brigades_report(0)
        except Exception:
            log.exception("[scheduler] loop error")
            await asyncio.sleep(5)

# ------------------------ Webhook plumbing ----------------
@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    await bot.set_my_commands([
        BotCommand(command="report_now", description="Ручний запуск звітів (/report_now [offset])"),
        BotCommand(command="where_am_i", description="Показати chat_id та thread_id"),
    ])

    url = f"{WEBHOOK_BASE}/webhook/{WEBHOOK_SECRET}"
    await bot.set_webhook(url)
    log.info("[startup] webhook set to %s", url)

    if SCHEDULER_ENABLED and LEADER:
        asyncio.create_task(scheduler_loop())
        log.info("[scheduler] enabled (LEADER=1)")
    else:
        log.info("[scheduler] disabled (SCHEDULER_ENABLED=%s, LEADER=%s)", SCHEDULER_ENABLED, LEADER)


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
