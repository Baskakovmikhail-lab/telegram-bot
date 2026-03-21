import os
import time
import random
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# =========================
# РќРђРЎРўР РћР™РљР
# =========================
API_TOKEN = os.getenv("API_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID_RAW = os.getenv("ADMIN_ID")

if not API_TOKEN:
    raise RuntimeError("РќРµ Р·Р°РґР°РЅ API_TOKEN")
if not CHANNEL_ID:
    raise RuntimeError("РќРµ Р·Р°РґР°РЅ CHANNEL_ID")
if not ADMIN_ID_RAW:
    raise RuntimeError("РќРµ Р·Р°РґР°РЅ ADMIN_ID")

ADMIN_ID = int(ADMIN_ID_RAW)

DB_PATH = "bot.db"
TZ = ZoneInfo("Asia/Yakutsk")
TIMEZONE_TEXT = "вЏ° Р’СЂРµРјСЏ Р±РѕС‚Р°: РЇРєСѓС‚СЃРє (UTC+9)"

MESSAGE_COOLDOWN_SECONDS = 2
MAX_WRONG_CODE_ATTEMPTS = 5
WRONG_CODE_BLOCK_MINUTES = 10
MAX_CAPTION_LENGTH = 1024
MAX_CODE_LENGTH = 64

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler(timezone=str(TZ))

print("Р‘РћРў Р—РђРџРЈР©Р•Рќ вњ…")


# =========================
# Р’Р Р•РњР•РќРќРћР• РЎРћРЎРўРћРЇРќРР•
# =========================
last_message_time = {}      # user_id -> timestamp
wrong_code_attempts = {}    # user_id -> {"count": int, "until": timestamp}
pending_confirm = set()     # user_id, РєРѕРјСѓ РїРѕРєР°Р·Р°РЅР° РєРЅРѕРїРєР° РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ

current_giveaway = {"active": False}
finish_job_id = None
start_job_id = None
finish_lock = asyncio.Lock()


# =========================
# FSM
# =========================
class GiveawayForm(StatesGroup):
    waiting_media = State()
    waiting_description = State()
    waiting_start_mode = State()
    waiting_start_datetime = State()
    waiting_end_mode = State()
    waiting_end_value = State()
    waiting_winners_count = State()
    waiting_code = State()
    waiting_confirm = State()


# =========================
# TIME HELPERS
# =========================
def now_tz() -> datetime:
    return datetime.now(TZ)


def to_storage_str(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.isoformat()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)


# =========================
# SQLITE
# =========================
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS giveaway (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                active INTEGER NOT NULL DEFAULT 0,
                media_type TEXT,
                media_file_id TEXT,
                description TEXT,
                start_mode TEXT,
                start_datetime TEXT,
                end_mode TEXT,
                end_value INTEGER,
                end_datetime TEXT,
                winners_count INTEGER,
                code TEXT,
                created_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS participants (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                joined_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS passed_confirm (
                user_id INTEGER PRIMARY KEY,
                giveaway_code TEXT,
                passed_at TEXT
            )
        """)

        cur.execute("""
            INSERT OR IGNORE INTO giveaway (id, active) VALUES (1, 0)
        """)

        conn.commit()


def save_giveaway(data: dict):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE giveaway
            SET active = ?,
                media_type = ?,
                media_file_id = ?,
                description = ?,
                start_mode = ?,
                start_datetime = ?,
                end_mode = ?,
                end_value = ?,
                end_datetime = ?,
                winners_count = ?,
                code = ?,
                created_at = ?
            WHERE id = 1
        """, (
            1 if data.get("active") else 0,
            data.get("media_type"),
            data.get("media_file_id"),
            data.get("description"),
            data.get("start_mode"),
            data.get("start_datetime"),
            data.get("end_mode"),
            data.get("end_value"),
            data.get("end_datetime"),
            data.get("winners_count"),
            data.get("code"),
            data.get("created_at"),
        ))
        conn.commit()


def load_giveaway() -> dict:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT active, media_type, media_file_id, description, start_mode,
                   start_datetime, end_mode, end_value, end_datetime,
                   winners_count, code, created_at
            FROM giveaway
            WHERE id = 1
        """)
        row = cur.fetchone()

    if not row:
        return {"active": False}

    return {
        "active": bool(row["active"]),
        "media_type": row["media_type"],
        "media_file_id": row["media_file_id"],
        "description": row["description"],
        "start_mode": row["start_mode"],
        "start_datetime": row["start_datetime"],
        "end_mode": row["end_mode"],
        "end_value": row["end_value"],
        "end_datetime": row["end_datetime"],
        "winners_count": row["winners_count"],
        "code": row["code"],
        "created_at": row["created_at"],
    }


def clear_giveaway_db():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE giveaway
            SET active = 0,
                media_type = NULL,
                media_file_id = NULL,
                description = NULL,
                start_mode = NULL,
                start_datetime = NULL,
                end_mode = NULL,
                end_value = NULL,
                end_datetime = NULL,
                winners_count = NULL,
                code = NULL,
                created_at = NULL
            WHERE id = 1
        """)
        conn.commit()


def add_participant(user_id: int, username: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO participants (user_id, username, joined_at)
            VALUES (?, ?, ?)
        """, (user_id, username, to_storage_str(now_tz())))
        conn.commit()


def participant_exists(user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM participants WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
    return row is not None


def get_participants() -> List[Tuple[int, str, str]]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, username, joined_at
            FROM participants
            ORDER BY joined_at ASC
        """)
        rows = cur.fetchall()
    return [(row["user_id"], row["username"], row["joined_at"]) for row in rows]


def get_participants_count() -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM participants")
        count = cur.fetchone()["cnt"]
    return count


def clear_participants():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM participants")
        conn.commit()


def mark_confirm_passed(user_id: int, giveaway_code: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO passed_confirm (user_id, giveaway_code, passed_at)
            VALUES (?, ?, ?)
        """, (user_id, giveaway_code, to_storage_str(now_tz())))
        conn.commit()


def has_passed_confirm(user_id: int, giveaway_code: str) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM passed_confirm
            WHERE user_id = ? AND giveaway_code = ?
        """, (user_id, giveaway_code))
        row = cur.fetchone()
    return row is not None


def clear_passed_confirm():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM passed_confirm")
        conn.commit()


# =========================
# HELPERS
# =========================
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def build_caption(description: str) -> str:
    return description.strip()


def normalize_code(value: str) -> str:
    return " ".join(value.strip().split())


def user_rate_limited(user_id: int) -> Optional[str]:
    now = time.time()

    if user_id in wrong_code_attempts:
        blocked_until = wrong_code_attempts[user_id]["until"]
        if blocked_until > now:
            minutes_left = int((blocked_until - now) // 60) + 1
            return f"РЎР»РёС€РєРѕРј РјРЅРѕРіРѕ РЅРµРІРµСЂРЅС‹С… РїРѕРїС‹С‚РѕРє. РџРѕРїСЂРѕР±СѓР№ С‡РµСЂРµР· {minutes_left} РјРёРЅ."

    last_ts = last_message_time.get(user_id, 0)
    if now - last_ts < MESSAGE_COOLDOWN_SECONDS:
        return "РЎР»РёС€РєРѕРј Р±С‹СЃС‚СЂРѕ. РџРѕРґРѕР¶РґРё РїР°СЂСѓ СЃРµРєСѓРЅРґ."

    last_message_time[user_id] = now
    return None


def register_wrong_code_attempt(user_id: int):
    now = time.time()
    current = wrong_code_attempts.get(user_id, {"count": 0, "until": 0})
    current["count"] += 1

    if current["count"] >= MAX_WRONG_CODE_ATTEMPTS:
        current["until"] = now + WRONG_CODE_BLOCK_MINUTES * 60
        current["count"] = 0

    wrong_code_attempts[user_id] = current


def reset_wrong_code_attempts(user_id: int):
    wrong_code_attempts.pop(user_id, None)


def admin_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("рџЋЃ РќРѕРІС‹Р№ СЂРѕР·С‹РіСЂС‹С€", callback_data="admin_new"),
        InlineKeyboardButton("рџ”Ґ РђРєС‚РёРІРЅС‹Р№ СЂРѕР·С‹РіСЂС‹С€", callback_data="admin_active"),
        InlineKeyboardButton("рџ“Љ РЎС‚Р°С‚СѓСЃ", callback_data="admin_status"),
        InlineKeyboardButton("рџ§ѕ РЎРїРёСЃРѕРє СѓС‡Р°СЃС‚РЅРёРєРѕРІ", callback_data="admin_list"),
        InlineKeyboardButton("вќЊ РћС‚РјРµРЅРёС‚СЊ СЂРѕР·С‹РіСЂС‹С€", callback_data="admin_cancel"),
    )
    return kb


def cancel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("вќЊ РћС‚РјРµРЅР°", callback_data="setup_cancel"))
    return kb


def start_mode_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("вљЎ РЎРµР№С‡Р°СЃ", callback_data="start_now"),
        InlineKeyboardButton("рџ•’ РџРѕ РІСЂРµРјРµРЅРё", callback_data="start_schedule"),
    )
    kb.add(InlineKeyboardButton("вќЊ РћС‚РјРµРЅР°", callback_data="setup_cancel"))
    return kb


def end_mode_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("рџ‘Ґ РџРѕ РєРѕР»РёС‡РµСЃС‚РІСѓ", callback_data="end_count"),
        InlineKeyboardButton("вЏ° РџРѕ РІСЂРµРјРµРЅРё", callback_data="end_time"),
    )
    kb.add(InlineKeyboardButton("вќЊ РћС‚РјРµРЅР°", callback_data="setup_cancel"))
    return kb


def confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("вњ… РџРѕРґС‚РІРµСЂРґРёС‚СЊ", callback_data="confirm_yes"),
        InlineKeyboardButton("вќЊ РћС‚РјРµРЅР°", callback_data="confirm_no"),
    )
    return kb


def publish_preview_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("рџљЂ РћРїСѓР±Р»РёРєРѕРІР°С‚СЊ", callback_data="publish_now"),
        InlineKeyboardButton("вќЊ РћС‚РјРµРЅР°", callback_data="publish_cancel"),
    )
    return kb


def human_check_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("РџРѕРґС‚РІРµСЂРґРёС‚СЊ СѓС‡Р°СЃС‚РёРµ вњ…", callback_data="captcha_ok"))
    return kb


async def send_step_hint(message: types.Message, text: str):
    await message.answer(
        f"{text}\n\nР•СЃР»Рё РїРµСЂРµРґСѓРјР°Р» вЂ” РЅР°Р¶РјРё В«РћС‚РјРµРЅР°В».",
        reply_markup=cancel_kb()
    )


async def send_step_hint_cb(callback: types.CallbackQuery, text: str):
    await callback.message.answer(
        f"{text}\n\nР•СЃР»Рё РїРµСЂРµРґСѓРјР°Р» вЂ” РЅР°Р¶РјРё В«РћС‚РјРµРЅР°В».",
        reply_markup=cancel_kb()
    )


async def animate_winner_selection(participants: List[Tuple[int, str, str]]) -> int:
    msg = await bot.send_message(ADMIN_ID, "рџЋІ Р—Р°РїСѓСЃРєР°СЋ СЂР°РЅРґРѕРјР°Р№Р·РµСЂ...")

    steps = [
        "рџЋІ РџСЂРѕРІРµСЂСЏСЋ СѓС‡Р°СЃС‚РЅРёРєРѕРІ...",
        "рџЋІ РЎС‡РёС‚Р°СЋ С€Р°РЅСЃС‹...",
        "рџЋІ Р’С‹Р±РёСЂР°СЋ СЃР»СѓС‡Р°Р№РЅРѕ...",
    ]

    for step in steps:
        await asyncio.sleep(0.8)
        await msg.edit_text(step)

    preview_names = [p[1] for p in participants]
    random.shuffle(preview_names)
    preview_names = preview_names[:min(3, len(preview_names))]

    for name in preview_names:
        await asyncio.sleep(0.6)
        await msg.edit_text(f"рџЋІ {name}")

    return msg.message_id


async def post_winners_to_channel(winners_text: str):
    try:
        await bot.send_message(
            CHANNEL_ID,
            f"рџЏ† Р РћР—Р«Р“Р Р«РЁ Р—РђР’Р•Р РЁРЃРќ\n\n"
            f"РџРѕР±РµРґРёС‚РµР»СЊ:\n{winners_text}\n\n"
            f"рџ’ё РџСЂРёР·: 1000в‚Ѕ\n\n"
            f"РЎРїР°СЃРёР±Рѕ РІСЃРµРј Р·Р° СѓС‡Р°СЃС‚РёРµ рџ”Ґ\n"
            f"РЎРєРѕСЂРѕ РЅРѕРІС‹Р№ СЂРѕР·С‹РіСЂС‹С€"
        )
    except Exception as e:
        logger.exception("РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ РїРѕР±РµРґРёС‚РµР»РµР№ РІ РєР°РЅР°Р»")
        await bot.send_message(ADMIN_ID, f"РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ РїРѕР±РµРґРёС‚РµР»РµР№ РІ РєР°РЅР°Р»: {e}")


async def remove_finish_job():
    global finish_job_id
    if finish_job_id:
        try:
            scheduler.remove_job(finish_job_id)
        except Exception:
            pass
        finish_job_id = None


async def remove_start_job():
    global start_job_id
    if start_job_id:
        try:
            scheduler.remove_job(start_job_id)
        except Exception:
            pass
        start_job_id = None


# =========================
# GIVEAWAY CORE
# =========================
async def finish_giveaway(reason: str):
    global current_giveaway

    async with finish_lock:
        if not current_giveaway.get("active"):
            return

        participants = get_participants()
        winners_count = current_giveaway.get("winners_count", 1) or 1

        await remove_finish_job()

        if not participants:
            await bot.send_message(
                ADMIN_ID,
                f"Р РѕР·С‹РіСЂС‹С€ Р·Р°РІРµСЂС€С‘РЅ ({reason}), РЅРѕ СѓС‡Р°СЃС‚РЅРёРєРѕРІ РЅРµС‚ вќЊ"
            )
            await post_winners_to_channel("РЈС‡Р°СЃС‚РЅРёРєРѕРІ РЅРµ Р±С‹Р»Рѕ.")
        else:
            msg_id = await animate_winner_selection(participants)

            ids = [row[0] for row in participants]
            names = {row[0]: row[1] for row in participants}

            actual_winners_count = min(winners_count, len(ids))
            winner_ids = random.sample(ids, actual_winners_count)

            medals = ["рџҐ‡", "рџҐ€", "рџҐ‰"]
            winners_lines = []
            for i, winner_id in enumerate(winner_ids, start=1):
                medal = medals[i - 1] if i <= len(medals) else f"{i}."
                winners_lines.append(f"{medal} {names[winner_id]}")

            winners_text = "\n".join(winners_lines)

            await bot.edit_message_text(
                f"рџЏ† РџРѕР±РµРґРёС‚РµР»СЊ РѕРїСЂРµРґРµР»С‘РЅ!\n\n{winners_text}",
                chat_id=ADMIN_ID,
                message_id=msg_id
            )

            await bot.send_message(
                ADMIN_ID,
                f"рџЋ‰ Р РѕР·С‹РіСЂС‹С€ Р·Р°РІРµСЂС€С‘РЅ!\n\n"
                f"РџСЂРёС‡РёРЅР°: {reason}\n"
                f"РЈС‡Р°СЃС‚РЅРёРєРѕРІ: {len(participants)}\n"
                f"РџРѕР±РµРґРёС‚РµР»РµР№: {actual_winners_count}\n\n"
                f"{winners_text}"
            )

            await post_winners_to_channel(winners_text)

        current_giveaway = {"active": False}
        clear_participants()
        clear_passed_confirm()
        clear_giveaway_db()
        pending_confirm.clear()


async def start_giveaway():
    global current_giveaway, finish_job_id

    if current_giveaway.get("active"):
        return

    clear_participants()
    clear_passed_confirm()
    pending_confirm.clear()

    media_type = current_giveaway["media_type"]
    media_file_id = current_giveaway["media_file_id"]
    description = build_caption(current_giveaway["description"])

    current_giveaway["active"] = True
    save_giveaway(current_giveaway)

    try:
        if media_type == "video":
            await bot.send_video(CHANNEL_ID, media_file_id, caption=description)
        else:
            await bot.send_photo(CHANNEL_ID, media_file_id, caption=description)
    except Exception:
        current_giveaway["active"] = False
        save_giveaway(current_giveaway)
        logger.exception("РћС€РёР±РєР° РїСѓР±Р»РёРєР°С†РёРё СЂРѕР·С‹РіСЂС‹С€Р°")
        await bot.send_message(ADMIN_ID, "вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСѓР±Р»РёРєРѕРІР°С‚СЊ СЂРѕР·С‹РіСЂС‹С€. РџСЂРѕРІРµСЂСЊ С„Р°Р№Р» Рё РїРѕРґРїРёСЃСЊ.")
        return

    await remove_start_job()
    await bot.send_message(ADMIN_ID, "вњ… Р РѕР·С‹РіСЂС‹С€ РѕРїСѓР±Р»РёРєРѕРІР°РЅ")

    if current_giveaway["end_mode"] == "time":
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            finish_job_id = f"finish_{int(time.time())}"
            scheduler.add_job(
                finish_giveaway,
                trigger="date",
                run_date=end_dt,
                args=["РІС‹С€Р»Рѕ РІСЂРµРјСЏ"],
                id=finish_job_id,
                replace_existing=True
            )


async def scheduled_start():
    await start_giveaway()


async def restore_state():
    global current_giveaway, finish_job_id, start_job_id

    current_giveaway = load_giveaway() or {"active": False}

    if not current_giveaway.get("start_mode"):
        current_giveaway = {"active": False}
        return

    start_dt = parse_dt(current_giveaway.get("start_datetime"))
    end_dt = parse_dt(current_giveaway.get("end_datetime"))
    now = now_tz()

    if current_giveaway.get("active"):
        if current_giveaway.get("end_mode") == "time" and end_dt:
            if now >= end_dt:
                await finish_giveaway("РІС‹С€Р»Рѕ РІСЂРµРјСЏ РїРѕСЃР»Рµ РїРµСЂРµР·Р°РїСѓСЃРєР°")
            else:
                finish_job_id = "finish_restored"
                scheduler.add_job(
                    finish_giveaway,
                    trigger="date",
                    run_date=end_dt,
                    args=["РІС‹С€Р»Рѕ РІСЂРµРјСЏ"],
                    id=finish_job_id,
                    replace_existing=True
                )
        return

    if current_giveaway.get("start_mode") == "scheduled" and start_dt:
        if now >= start_dt:
            if current_giveaway.get("end_mode") == "time" and end_dt and now >= end_dt:
                clear_participants()
                clear_passed_confirm()
                clear_giveaway_db()
                pending_confirm.clear()
                current_giveaway = {"active": False}
                await bot.send_message(ADMIN_ID, "в„№пёЏ Р—Р°РїР»Р°РЅРёСЂРѕРІР°РЅРЅС‹Р№ СЂРѕР·С‹РіСЂС‹С€ РёСЃС‚С‘Рє РІРѕ РІСЂРµРјСЏ РїСЂРѕСЃС‚РѕСЏ Р±РѕС‚Р° Рё Р±С‹Р» СЃР±СЂРѕС€РµРЅ.")
            else:
                await start_giveaway()
        else:
            start_job_id = "start_restored"
            scheduler.add_job(
                scheduled_start,
                trigger="date",
                run_date=start_dt,
                id=start_job_id,
                replace_existing=True
            )


async def on_startup(_):
    init_db()
    scheduler.start()
    await restore_state()
    await bot.send_message(ADMIN_ID, "РџР»Р°РЅРёСЂРѕРІС‰РёРє Р·Р°РїСѓС‰РµРЅ вњ…")


# =========================
# ADMIN COMMANDS
# =========================
@dp.message_handler(commands=["admin"], state="*")
async def admin_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await state.finish()
    await message.answer("вљ™пёЏ РђРґРјРёРЅ-РјРµРЅСЋ", reply_markup=admin_menu_kb())


@dp.message_handler(commands=["cancelsetup"], state="*")
async def cancel_setup_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await state.finish()
    await message.answer("РЎРѕР·РґР°РЅРёРµ СЂРѕР·С‹РіСЂС‹С€Р° РѕС‚РјРµРЅРµРЅРѕ вќЊ", reply_markup=admin_menu_kb())


@dp.message_handler(commands=["cancel"])
async def cancel_cmd(message: types.Message):
    global current_giveaway

    if not is_admin(message.from_user.id):
        return

    await remove_finish_job()
    await remove_start_job()

    clear_participants()
    clear_passed_confirm()
    clear_giveaway_db()
    current_giveaway = {"active": False}
    pending_confirm.clear()

    await message.answer("Р РѕР·С‹РіСЂС‹С€ РѕС‚РјРµРЅС‘РЅ вќЊ")


# =========================
# USER COMMANDS
# =========================
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    await message.answer(
        "РџСЂРёРІРµС‚ рџ‘‹\n\n"
        "Р§С‚РѕР±С‹ СѓС‡Р°СЃС‚РІРѕРІР°С‚СЊ:\n"
        "1. РџРѕРґРїРёС€РёСЃСЊ РЅР° РєР°РЅР°Р»\n"
        "2. РќР°Р№РґРё РєРѕРґ\n"
        "3. РћС‚РїСЂР°РІСЊ РєРѕРґ СЃСЋРґР°\n"
        "4. РќР°Р¶РјРё РєРЅРѕРїРєСѓ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ"
    )


@dp.message_handler(commands=["status"])
async def status_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    if not current_giveaway.get("active"):
        await message.answer("РЎРµР№С‡Р°СЃ Р°РєС‚РёРІРЅРѕРіРѕ СЂРѕР·С‹РіСЂС‹С€Р° РЅРµС‚")
        return

    text = (
        f"рџ“Љ РЎС‚Р°С‚СѓСЃ СЂРѕР·С‹РіСЂС‹С€Р°\n\n"
        f"РљРѕРґ: {current_giveaway.get('code')}\n"
        f"РЈС‡Р°СЃС‚РЅРёРєРѕРІ: {get_participants_count()}\n"
        f"РџРѕР±РµРґРёС‚РµР»РµР№: {current_giveaway.get('winners_count')}\n"
        f"Р—Р°РІРµСЂС€РµРЅРёРµ: {'РїРѕ РєРѕР»РёС‡РµСЃС‚РІСѓ' if current_giveaway.get('end_mode') == 'count' else 'РїРѕ РІСЂРµРјРµРЅРё'}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Р¦РµР»СЊ: {current_giveaway.get('end_value')}"
    else:
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            text += f"Р”Рѕ: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}\n{TIMEZONE_TEXT}"

    await message.answer(text)


# =========================
# ADMIN CALLBACKS
# =========================
@dp.callback_query_handler(lambda c: c.data == "admin_new")
async def cb_admin_new(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    if current_giveaway.get("active"):
        await callback.message.answer("РЎРµР№С‡Р°СЃ СѓР¶Рµ РµСЃС‚СЊ Р°РєС‚РёРІРЅС‹Р№ СЂРѕР·С‹РіСЂС‹С€ вќ—")
        await callback.answer()
        return

    await GiveawayForm.waiting_media.set()
    await send_step_hint_cb(
        callback,
        "РћС‚РїСЂР°РІСЊ С„РѕС‚Рѕ РёР»Рё РІРёРґРµРѕ РґР»СЏ РїРѕСЃС‚Р°.\n\nРњРѕР¶РЅРѕ РїСЂРёСЃР»Р°С‚СЊ РєР°Рє РјРµРґРёР° РёР»Рё РєР°Рє С„Р°Р№Р»."
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_active")
async def cb_admin_active(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    if not current_giveaway.get("active"):
        await callback.message.answer("РЎРµР№С‡Р°СЃ Р°РєС‚РёРІРЅРѕРіРѕ СЂРѕР·С‹РіСЂС‹С€Р° РЅРµС‚")
        await callback.answer()
        return

    text = (
        f"рџ”Ґ РђРљРўРР’РќР«Р™ Р РћР—Р«Р“Р Р«РЁ\n\n"
        f"РљРѕРґ: {current_giveaway.get('code')}\n"
        f"РЈС‡Р°СЃС‚РЅРёРєРѕРІ: {get_participants_count()}\n"
        f"РџРѕР±РµРґРёС‚РµР»РµР№: {current_giveaway.get('winners_count')}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Р¦РµР»СЊ: {current_giveaway.get('end_value')}"
    else:
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            text += f"Р”Рѕ: {end_dt.strftime('%Y-%m-%d %H:%M')}\n{TIMEZONE_TEXT}"

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("рџ§ѕ РЈС‡Р°СЃС‚РЅРёРєРё", callback_data="admin_list"),
        InlineKeyboardButton("рџ›‘ Р—Р°РІРµСЂС€РёС‚СЊ СЃРµР№С‡Р°СЃ", callback_data="admin_finish_now"),
    )
    kb.add(InlineKeyboardButton("вќЊ РћС‚РјРµРЅРёС‚СЊ", callback_data="admin_cancel"))

    await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_finish_now")
async def cb_finish_now(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    await finish_giveaway("Р·Р°РІРµСЂС€РµРЅРѕ РІСЂСѓС‡РЅСѓСЋ")
    await callback.answer("Р РѕР·С‹РіСЂС‹С€ Р·Р°РІРµСЂС€С‘РЅ")


@dp.callback_query_handler(lambda c: c.data == "admin_status")
async def cb_admin_status(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    if not current_giveaway.get("active"):
        await callback.message.answer("РЎРµР№С‡Р°СЃ Р°РєС‚РёРІРЅРѕРіРѕ СЂРѕР·С‹РіСЂС‹С€Р° РЅРµС‚")
        await callback.answer()
        return

    text = (
        f"рџ“Љ РЎС‚Р°С‚СѓСЃ СЂРѕР·С‹РіСЂС‹С€Р°\n\n"
        f"РљРѕРґ: {current_giveaway.get('code')}\n"
        f"РЈС‡Р°СЃС‚РЅРёРєРѕРІ: {get_participants_count()}\n"
        f"РџРѕР±РµРґРёС‚РµР»РµР№: {current_giveaway.get('winners_count')}\n"
        f"Р—Р°РІРµСЂС€РµРЅРёРµ: {'РїРѕ РєРѕР»РёС‡РµСЃС‚РІСѓ' if current_giveaway.get('end_mode') == 'count' else 'РїРѕ РІСЂРµРјРµРЅРё'}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Р¦РµР»СЊ: {current_giveaway.get('end_value')}"
    else:
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            text += f"Р”Рѕ: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}\n{TIMEZONE_TEXT}"

    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_list")
async def cb_admin_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    participants = get_participants()

    if not participants:
        await callback.message.answer("РЈС‡Р°СЃС‚РЅРёРєРѕРІ РїРѕРєР° РЅРµС‚")
        await callback.answer()
        return

    lines = ["рџ§ѕ РЎРїРёСЃРѕРє СѓС‡Р°СЃС‚РЅРёРєРѕРІ:\n"]
    for i, (_, username, joined_at) in enumerate(participants[:50], start=1):
        lines.append(f"{i}. {username} вЂ” {joined_at[:19].replace('T', ' ')}")

    if len(participants) > 50:
        lines.append(f"\nР РµС‰С‘ {len(participants) - 50} СѓС‡Р°СЃС‚РЅРёРєРѕРІ...")

    await callback.message.answer("\n".join(lines))
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_cancel")
async def cb_admin_cancel(callback: types.CallbackQuery):
    global current_giveaway

    if not is_admin(callback.from_user.id):
        return

    await remove_finish_job()
    await remove_start_job()

    clear_participants()
    clear_passed_confirm()
    clear_giveaway_db()
    current_giveaway = {"active": False}
    pending_confirm.clear()

    await callback.message.answer("РђРєС‚РёРІРЅС‹Р№ СЂРѕР·С‹РіСЂС‹С€ РѕС‚РјРµРЅС‘РЅ вќЊ")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "setup_cancel", state="*")
async def cb_setup_cancel(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    await state.finish()
    await callback.message.answer("РЎРѕР·РґР°РЅРёРµ СЂРѕР·С‹РіСЂС‹С€Р° РѕС‚РјРµРЅРµРЅРѕ вќЊ", reply_markup=admin_menu_kb())
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "publish_now")
async def cb_publish_now(callback: types.CallbackQuery):
    global start_job_id

    if not is_admin(callback.from_user.id):
        return

    if not current_giveaway or not current_giveaway.get("media_file_id"):
        await callback.answer("РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ РїСѓР±Р»РёРєР°С†РёРё", show_alert=True)
        return

    if current_giveaway["start_mode"] == "now":
        await start_giveaway()
        await callback.message.answer("вњ… Р РѕР·С‹РіСЂС‹С€ РѕРїСѓР±Р»РёРєРѕРІР°РЅ", reply_markup=admin_menu_kb())
    else:
        run_date = parse_dt(current_giveaway["start_datetime"])
        start_job_id = f"start_{int(time.time())}"
        scheduler.add_job(
            scheduled_start,
            trigger="date",
            run_date=run_date,
            id=start_job_id,
            replace_existing=True
        )
        await callback.message.answer(
            f"вњ… Р РѕР·С‹РіСЂС‹С€ Р·Р°РїР»Р°РЅРёСЂРѕРІР°РЅ РЅР° "
            f"{run_date.strftime('%Y-%m-%d %H:%M')}\n"
            f"{TIMEZONE_TEXT}",
            reply_markup=admin_menu_kb()
        )

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "publish_cancel")
async def cb_publish_cancel(callback: types.CallbackQuery):
    global current_giveaway

    if not is_admin(callback.from_user.id):
        return

    await remove_start_job()
    clear_giveaway_db()
    current_giveaway = {"active": False}

    await callback.message.answer("РџСѓР±Р»РёРєР°С†РёСЏ РѕС‚РјРµРЅРµРЅР° вќЊ", reply_markup=admin_menu_kb())
    await callback.answer()


# =========================
# FSM: РћРЎРќРћР’РќРћР™ FLOW
# =========================
@dp.message_handler(content_types=["video", "photo", "document"], state=GiveawayForm.waiting_media)
async def process_media(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    media_type = None
    file_id = None

    if message.video:
        media_type = "video"
        file_id = message.video.file_id
    elif message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
    elif message.document:
        mime = (message.document.mime_type or "").lower()
        name = (message.document.file_name or "").lower()
        if mime.startswith("video/") or name.endswith((".mp4", ".mov", ".mkv")):
            media_type = "video"
        elif mime.startswith("image/") or name.endswith((".jpg", ".jpeg", ".png", ".webp")):
            media_type = "photo"
        else:
            await send_step_hint(
                message,
                "РџРѕРґС…РѕРґРёС‚ С‚РѕР»СЊРєРѕ С„РѕС‚Рѕ РёР»Рё РІРёРґРµРѕ.\n\nРћС‚РїСЂР°РІСЊ РґСЂСѓРіРѕР№ С„Р°Р№Р»."
            )
            return
        file_id = message.document.file_id
    else:
        await send_step_hint(message, "РћС‚РїСЂР°РІСЊ С„РѕС‚Рѕ РёР»Рё РІРёРґРµРѕ.")
        return

    await state.update_data(media_type=media_type, media_file_id=file_id)
    await GiveawayForm.waiting_description.set()
    await send_step_hint(message, f"РўРµРїРµСЂСЊ РѕС‚РїСЂР°РІСЊ РѕРїРёСЃР°РЅРёРµ РїРѕСЃС‚Р° (РґРѕ {MAX_CAPTION_LENGTH} СЃРёРјРІРѕР»РѕРІ)")


@dp.message_handler(state=GiveawayForm.waiting_description, content_types=types.ContentTypes.TEXT)
async def process_description(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()

    if not text:
        await send_step_hint(message, "РћРїРёСЃР°РЅРёРµ РЅРµ РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ РїСѓСЃС‚С‹Рј. РћС‚РїСЂР°РІСЊ С‚РµРєСЃС‚ РїРѕСЃС‚Р°.")
        return

    if len(text) > MAX_CAPTION_LENGTH:
        await send_step_hint(message, f"РћРїРёСЃР°РЅРёРµ СЃР»РёС€РєРѕРј РґР»РёРЅРЅРѕРµ. РњР°РєСЃРёРјСѓРј {MAX_CAPTION_LENGTH} СЃРёРјРІРѕР»Р°.")
        return

    await state.update_data(description=text)
    await GiveawayForm.waiting_start_mode.set()
    await message.answer("РљРѕРіРґР° Р·Р°РїСѓСЃРєР°С‚СЊ СЂРѕР·С‹РіСЂС‹С€?", reply_markup=start_mode_kb())


@dp.callback_query_handler(lambda c: c.data in ["start_now", "start_schedule"], state=GiveawayForm.waiting_start_mode)
async def process_start_mode(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "start_now":
        await state.update_data(start_mode="now", start_datetime=None)
        await GiveawayForm.waiting_end_mode.set()
        await callback.message.answer("РљР°Рє Р·Р°РІРµСЂС€Р°С‚СЊ СЂРѕР·С‹РіСЂС‹С€?", reply_markup=end_mode_kb())
    else:
        await state.update_data(start_mode="scheduled")
        await GiveawayForm.waiting_start_datetime.set()
        await send_step_hint_cb(
            callback,
            f"Р’РІРµРґРё РґР°С‚Сѓ Рё РІСЂРµРјСЏ СЃС‚Р°СЂС‚Р° РІ С„РѕСЂРјР°С‚Рµ:\n2026-03-20 21:30\n\n{TIMEZONE_TEXT}"
        )
    await callback.answer()


@dp.message_handler(state=GiveawayForm.waiting_start_datetime, content_types=types.ContentTypes.TEXT)
async def process_start_datetime(message: types.Message, state: FSMContext):
    try:
        naive_start_dt = datetime.strptime(message.text.strip(), "%Y-%m-%d %H:%M")
        start_dt = naive_start_dt.replace(tzinfo=TZ)
    except ValueError:
        await send_step_hint(
            message,
            f"РќРµРІРµСЂРЅС‹Р№ С„РѕСЂРјР°С‚.\nРџСЂРёРјРµСЂ: 2026-03-20 21:30\n\n{TIMEZONE_TEXT}"
        )
        return

    if start_dt <= now_tz():
        await send_step_hint(
            message,
            f"Р’СЂРµРјСЏ СЃС‚Р°СЂС‚Р° РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ РІ Р±СѓРґСѓС‰РµРј.\n\n{TIMEZONE_TEXT}"
        )
        return

    await state.update_data(start_datetime=to_storage_str(start_dt))
    await GiveawayForm.waiting_end_mode.set()
    await message.answer("РљР°Рє Р·Р°РІРµСЂС€Р°С‚СЊ СЂРѕР·С‹РіСЂС‹С€?", reply_markup=end_mode_kb())


@dp.callback_query_handler(lambda c: c.data in ["end_count", "end_time"], state=GiveawayForm.waiting_end_mode)
async def process_end_mode(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "end_count":
        await state.update_data(end_mode="count")
        await GiveawayForm.waiting_end_value.set()
        await send_step_hint_cb(
            callback,
            "РЎРєРѕР»СЊРєРѕ СѓС‡Р°СЃС‚РЅРёРєРѕРІ РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ РґРѕ Р·Р°РІРµСЂС€РµРЅРёСЏ?"
        )
    else:
        await state.update_data(end_mode="time")
        await GiveawayForm.waiting_end_value.set()
        await send_step_hint_cb(
            callback,
            f"РЎРєРѕР»СЊРєРѕ РјРёРЅСѓС‚ РґРѕР»Р¶РµРЅ РёРґС‚Рё СЂРѕР·С‹РіСЂС‹С€ РїРѕСЃР»Рµ СЃС‚Р°СЂС‚Р°?\n\n{TIMEZONE_TEXT}"
        )
    await callback.answer()


@dp.message_handler(state=GiveawayForm.waiting_end_value, content_types=types.ContentTypes.TEXT)
async def process_end_value(message: types.Message, state: FSMContext):
    try:
        value = int(message.text.strip())
    except ValueError:
        await send_step_hint(message, "РќСѓР¶РЅРѕ РІРІРµСЃС‚Рё С‡РёСЃР»Рѕ.")
        return

    if value <= 0:
        await send_step_hint(message, "Р§РёСЃР»Рѕ РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ Р±РѕР»СЊС€Рµ РЅСѓР»СЏ.")
        return

    await state.update_data(end_value=value)
    await GiveawayForm.waiting_winners_count.set()
    await send_step_hint(message, "РЎРєРѕР»СЊРєРѕ РїРѕР±РµРґРёС‚РµР»РµР№ РІС‹Р±СЂР°С‚СЊ?")


@dp.message_handler(state=GiveawayForm.waiting_winners_count, content_types=types.ContentTypes.TEXT)
async def process_winners_count(message: types.Message, state: FSMContext):
    try:
        winners_count = int(message.text.strip())
    except ValueError:
        await send_step_hint(message, "РќСѓР¶РЅРѕ РІРІРµСЃС‚Рё С‡РёСЃР»Рѕ.")
        return

    if winners_count <= 0:
        await send_step_hint(message, "РљРѕР»РёС‡РµСЃС‚РІРѕ РїРѕР±РµРґРёС‚РµР»РµР№ РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ Р±РѕР»СЊС€Рµ РЅСѓР»СЏ.")
        return

    await state.update_data(winners_count=winners_count)
    await GiveawayForm.waiting_code.set()
    await send_step_hint(message, f"РўРµРїРµСЂСЊ РѕС‚РїСЂР°РІСЊ РєРѕРґ, РєРѕС‚РѕСЂС‹Р№ СѓС‡Р°СЃС‚РЅРёРєРё Р±СѓРґСѓС‚ РІРІРѕРґРёС‚СЊ РІ Р±РѕС‚Р° (РґРѕ {MAX_CODE_LENGTH} СЃРёРјРІРѕР»РѕРІ)")


@dp.message_handler(state=GiveawayForm.waiting_code, content_types=types.ContentTypes.TEXT)
async def process_code(message: types.Message, state: FSMContext):
    code = normalize_code(message.text or "")

    if not code:
        await send_step_hint(message, "РљРѕРґ РЅРµ РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ РїСѓСЃС‚С‹Рј.")
        return

    if len(code) > MAX_CODE_LENGTH:
        await send_step_hint(message, f"РљРѕРґ СЃР»РёС€РєРѕРј РґР»РёРЅРЅС‹Р№. РњР°РєСЃРёРјСѓРј {MAX_CODE_LENGTH} СЃРёРјРІРѕР»Р°.")
        return

    data = await state.get_data()
    await state.update_data(code=code)

    start_mode = data.get("start_mode")
    start_datetime = data.get("start_datetime")
    end_mode = data.get("end_mode")
    end_value = data.get("end_value")
    winners_count = data.get("winners_count")
    media_type = data.get("media_type")
    description = data.get("description")

    start_text = "СЃРµР№С‡Р°СЃ" if start_mode == "now" else parse_dt(start_datetime).strftime("%Y-%m-%d %H:%M")
    end_text = "РїРѕ РєРѕР»РёС‡РµСЃС‚РІСѓ" if end_mode == "count" else "РїРѕ РІСЂРµРјРµРЅРё"

    summary = (
        f"РџСЂРѕРІРµСЂСЊ РЅР°СЃС‚СЂРѕР№РєРё СЂРѕР·С‹РіСЂС‹С€Р°:\n\n"
        f"Р¤Р°Р№Р»: {media_type}\n"
        f"РћРїРёСЃР°РЅРёРµ: {description}\n"
        f"РЎС‚Р°СЂС‚: {start_text}\n"
        f"Р—Р°РІРµСЂС€РµРЅРёРµ: {end_text}\n"
        f"Р—РЅР°С‡РµРЅРёРµ: {end_value}\n"
        f"РџРѕР±РµРґРёС‚РµР»РµР№: {winners_count}\n"
        f"РљРѕРґ: {code}\n"
    )

    if start_mode != "now":
        summary += f"\n{TIMEZONE_TEXT}"

    await GiveawayForm.waiting_confirm.set()
    await message.answer(summary, reply_markup=confirm_kb())


# =========================
# FSM: FALLBACK Р”Р›РЇ РђР”РњРРќРђ
# =========================
@dp.message_handler(state=GiveawayForm.waiting_media)
async def fallback_waiting_media(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "РЎРµР№С‡Р°СЃ Р±РѕС‚ Р¶РґС‘С‚ С„РѕС‚Рѕ РёР»Рё РІРёРґРµРѕ РґР»СЏ РїРѕСЃС‚Р°.")


@dp.message_handler(state=GiveawayForm.waiting_description)
async def fallback_waiting_description(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "РЎРµР№С‡Р°СЃ Р±РѕС‚ Р¶РґС‘С‚ РѕРїРёСЃР°РЅРёРµ РїРѕСЃС‚Р° С‚РµРєСЃС‚РѕРј.")


@dp.message_handler(state=GiveawayForm.waiting_start_mode)
async def fallback_waiting_start_mode(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "РЎРµР№С‡Р°СЃ РІС‹Р±РµСЂРё СЃРїРѕСЃРѕР± СЃС‚Р°СЂС‚Р° РєРЅРѕРїРєР°РјРё РЅРёР¶Рµ.\n\nР•СЃР»Рё РїРµСЂРµРґСѓРјР°Р» вЂ” РЅР°Р¶РјРё В«РћС‚РјРµРЅР°В».",
        reply_markup=start_mode_kb()
    )


@dp.message_handler(state=GiveawayForm.waiting_start_datetime)
async def fallback_waiting_start_datetime(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(
        message,
        f"РЎРµР№С‡Р°СЃ Р±РѕС‚ Р¶РґС‘С‚ РґР°С‚Сѓ Рё РІСЂРµРјСЏ СЃС‚Р°СЂС‚Р° РІ С„РѕСЂРјР°С‚Рµ:\n2026-03-20 21:30\n\n{TIMEZONE_TEXT}"
    )


@dp.message_handler(state=GiveawayForm.waiting_end_mode)
async def fallback_waiting_end_mode(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "РЎРµР№С‡Р°СЃ РІС‹Р±РµСЂРё СЃРїРѕСЃРѕР± Р·Р°РІРµСЂС€РµРЅРёСЏ РєРЅРѕРїРєР°РјРё РЅРёР¶Рµ.\n\nР•СЃР»Рё РїРµСЂРµРґСѓРјР°Р» вЂ” РЅР°Р¶РјРё В«РћС‚РјРµРЅР°В».",
        reply_markup=end_mode_kb()
    )


@dp.message_handler(state=GiveawayForm.waiting_end_value)
async def fallback_waiting_end_value(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "РЎРµР№С‡Р°СЃ Р±РѕС‚ Р¶РґС‘С‚ С‡РёСЃР»Рѕ РґР»СЏ Р·Р°РІРµСЂС€РµРЅРёСЏ СЂРѕР·С‹РіСЂС‹С€Р°.")


@dp.message_handler(state=GiveawayForm.waiting_winners_count)
async def fallback_waiting_winners_count(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "РЎРµР№С‡Р°СЃ Р±РѕС‚ Р¶РґС‘С‚ РєРѕР»РёС‡РµСЃС‚РІРѕ РїРѕР±РµРґРёС‚РµР»РµР№.")


@dp.message_handler(state=GiveawayForm.waiting_code)
async def fallback_waiting_code(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "РЎРµР№С‡Р°СЃ Р±РѕС‚ Р¶РґС‘С‚ РєРѕРґ РґР»СЏ СѓС‡Р°СЃС‚РЅРёРєРѕРІ.")


@dp.message_handler(state=GiveawayForm.waiting_confirm)
async def fallback_waiting_confirm(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "РЎРµР№С‡Р°СЃ РїСЂРѕРІРµСЂСЊ РЅР°СЃС‚СЂРѕР№РєРё Рё РЅР°Р¶РјРё РѕРґРЅСѓ РёР· РєРЅРѕРїРѕРє РЅРёР¶Рµ.\n\nР•СЃР»Рё РїРµСЂРµРґСѓРјР°Р» вЂ” РЅР°Р¶РјРё В«РћС‚РјРµРЅР°В».",
        reply_markup=confirm_kb()
    )


# =========================
# РџРћР”РўР’Р•Р Р–Р”Р•РќРР• Р РћР—Р«Р“Р Р«РЁРђ
# =========================
@dp.callback_query_handler(lambda c: c.data in ["confirm_yes", "confirm_no"], state=GiveawayForm.waiting_confirm)
async def process_confirm(callback: types.CallbackQuery, state: FSMContext):
    global current_giveaway

    if callback.data == "confirm_no":
        await state.finish()
        await callback.message.answer("РЎРѕР·РґР°РЅРёРµ СЂРѕР·С‹РіСЂС‹С€Р° РѕС‚РјРµРЅРµРЅРѕ вќЊ", reply_markup=admin_menu_kb())
        await callback.answer()
        return

    data = await state.get_data()

    current_giveaway = {
        "active": False,
        "media_type": data["media_type"],
        "media_file_id": data["media_file_id"],
        "description": data["description"],
        "start_mode": data["start_mode"],
        "start_datetime": data.get("start_datetime"),
        "end_mode": data["end_mode"],
        "end_value": data["end_value"],
        "winners_count": data["winners_count"],
        "code": data["code"],
        "created_at": to_storage_str(now_tz()),
    }

    if current_giveaway["end_mode"] == "time":
        if current_giveaway["start_mode"] == "now":
            current_giveaway["end_datetime"] = to_storage_str(
                now_tz() + timedelta(minutes=current_giveaway["end_value"])
            )
        else:
            start_dt = parse_dt(current_giveaway["start_datetime"])
            current_giveaway["end_datetime"] = to_storage_str(
                start_dt + timedelta(minutes=current_giveaway["end_value"])
            )
    else:
        current_giveaway["end_datetime"] = None

    save_giveaway(current_giveaway)

    caption = build_caption(current_giveaway["description"])
    if current_giveaway["media_type"] == "video":
        await bot.send_video(ADMIN_ID, current_giveaway["media_file_id"], caption=caption)
    else:
        await bot.send_photo(ADMIN_ID, current_giveaway["media_file_id"], caption=caption)

    await callback.message.answer(
        "рџ‘† РўР°Рє Р±СѓРґРµС‚ РІС‹РіР»СЏРґРµС‚СЊ РїРѕСЃС‚\n\nР•СЃР»Рё РІСЃС‘ РѕРє вЂ” РЅР°Р¶РјРё В«РћРїСѓР±Р»РёРєРѕРІР°С‚СЊВ».",
        reply_markup=publish_preview_kb()
    )

    await state.finish()
    await callback.answer()


# =========================
# РџРћР”РўР’Р•Р Р–Р”Р•РќРР• РЈР§РђРЎРўРРЇ
# =========================
@dp.callback_query_handler(lambda c: c.data == "captcha_ok")
async def captcha_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    current_code = current_giveaway.get("code")

    if not current_giveaway.get("active"):
        await callback.answer("РЎРµР№С‡Р°СЃ РЅРµС‚ Р°РєС‚РёРІРЅРѕРіРѕ СЂРѕР·С‹РіСЂС‹С€Р°", show_alert=True)
        return

    if user_id not in pending_confirm:
        await callback.answer("РџРѕРґС‚РІРµСЂР¶РґРµРЅРёРµ СѓР¶Рµ РЅРµР°РєС‚СѓР°Р»СЊРЅРѕ", show_alert=True)
        return

    username = callback.from_user.username
    full_name = callback.from_user.full_name
    display_name = f"@{username}" if username else full_name

    if not participant_exists(user_id):
        add_participant(user_id, display_name)

    mark_confirm_passed(user_id, current_code)
    pending_confirm.discard(user_id)
    reset_wrong_code_attempts(user_id)

    await callback.message.edit_text("Р“РѕС‚РѕРІРѕ вњ…\nРўС‹ СѓС‡Р°СЃС‚РІСѓРµС€СЊ РІ СЂРѕР·С‹РіСЂС‹С€Рµ")
    await callback.answer("РЈС‡Р°СЃС‚РёРµ РїРѕРґС‚РІРµСЂР¶РґРµРЅРѕ")

    if current_giveaway.get("end_mode") == "count":
        if get_participants_count() >= current_giveaway.get("end_value"):
            await finish_giveaway("РЅР°Р±СЂР°РЅРѕ РЅСѓР¶РЅРѕРµ РєРѕР»РёС‡РµСЃС‚РІРѕ СѓС‡Р°СЃС‚РЅРёРєРѕРІ")


# =========================
# РЈР§РђРЎРўРР• РџРћР›Р¬Р—РћР’РђРўР•Р›РЇ
# =========================
@dp.message_handler()
async def check_code(message: types.Message):
    if not current_giveaway.get("active"):
        await message.answer("РЎРµР№С‡Р°СЃ РЅРµС‚ Р°РєС‚РёРІРЅРѕРіРѕ СЂРѕР·С‹РіСЂС‹С€Р°")
        return

    user_id = message.from_user.id

    limiter_message = user_rate_limited(user_id)
    if limiter_message:
        await message.answer(limiter_message)
        return

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
    except Exception:
        logger.exception("РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РїРѕРґРїРёСЃРєРё")
        await message.answer("РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РїРѕРґРїРёСЃРєРё рџ•")
        return

    if member.status not in ["member", "creator", "administrator"]:
        await message.answer("РЎРЅР°С‡Р°Р»Р° РїРѕРґРїРёС€РёСЃСЊ РЅР° РєР°РЅР°Р» вќ—")
        return

    text = normalize_code(message.text or "")
    current_code = current_giveaway.get("code")

    if text != current_code:
        register_wrong_code_attempt(user_id)
        await message.answer("РќРµРІРµСЂРЅС‹Р№ РєРѕРґ вќЊ\nРџСЂРѕРІРµСЂСЊ РІРЅРёРјР°С‚РµР»СЊРЅРѕ Рё РїРѕРїСЂРѕР±СѓР№ РµС‰С‘ СЂР°Р·")
        return

    if participant_exists(user_id):
        await message.answer("РўС‹ СѓР¶Рµ СѓС‡Р°СЃС‚РІСѓРµС€СЊ РІ СЌС‚РѕРј СЂРѕР·С‹РіСЂС‹С€Рµ рџЋ‰")
        return

    if has_passed_confirm(user_id, current_code):
        username = message.from_user.username
        full_name = message.from_user.full_name
        display_name = f"@{username}" if username else full_name

        add_participant(user_id, display_name)
        await message.answer("Р“РѕС‚РѕРІРѕ вњ…\nРўС‹ СѓС‡Р°СЃС‚РІСѓРµС€СЊ РІ СЂРѕР·С‹РіСЂС‹С€Рµ")
        return

    pending_confirm.add(user_id)
    await message.answer(
        "РљРѕРґ РїСЂРёРЅСЏС‚ вњ…\nРџРѕРґС‚РІРµСЂРґРё СѓС‡Р°СЃС‚РёРµ рџ‘‡",
        reply_markup=human_check_kb()
    )


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)