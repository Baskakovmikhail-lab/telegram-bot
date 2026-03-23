import os
import time
import random
import asyncio
import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple, Set

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.exceptions import RetryAfter, MessageNotModified
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# =========================
# НАСТРОЙКИ
# =========================
API_TOKEN = os.getenv("API_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID_RAW = os.getenv("ADMIN_ID")

if not API_TOKEN:
    raise RuntimeError("Не задан API_TOKEN")
if not CHANNEL_ID:
    raise RuntimeError("Не задан CHANNEL_ID")
if not ADMIN_ID_RAW:
    raise RuntimeError("Не задан ADMIN_ID")

ADMIN_ID = int(ADMIN_ID_RAW)
DB_PATH = "bot.db"

UTC_PLUS_3 = timezone(timedelta(hours=3))
TIMEZONE_TEXT = "⏰ Время бота: UTC+3"

MESSAGE_COOLDOWN_SECONDS = 2
MAX_WRONG_CODE_ATTEMPTS = 5
WRONG_CODE_BLOCK_MINUTES = 10

NUMBER_MIN = 1
DEFAULT_NUMBER_MAX = 100

STATUS_JOB_ID = "giveaway_status_updater"
START_JOB_ID_RESTORED = "start_restored"
FINISH_JOB_ID_RESTORED = "finish_restored"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("giveaway_bot")

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler(timezone=UTC_PLUS_3)

logger.info("БОТ ЗАПУЩЕН ✅")


# =========================
# ВРЕМЕННОЕ СОСТОЯНИЕ
# =========================
last_message_time = {}
wrong_code_attempts = {}
pending_number_choice: Set[int] = set()

current_giveaway = {"active": False}
finish_job_id = None
start_job_id = None

lifecycle_lock = asyncio.Lock()
join_lock = asyncio.Lock()
status_lock = asyncio.Lock()


# =========================
# FSM
# =========================
class GiveawayForm(StatesGroup):
    waiting_media = State()
    waiting_description = State()
    waiting_start_mode = State()
    waiting_start_datetime = State()
    waiting_end_mode = State()
    waiting_end_count = State()
    waiting_end_datetime = State()
    waiting_winners_count = State()
    waiting_code = State()


class NumberChoiceForm(StatesGroup):
    waiting_number = State()


# =========================
# TIME HELPERS
# =========================
def now_utc3() -> datetime:
    return datetime.now(UTC_PLUS_3)


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC_PLUS_3)
    return dt.astimezone(UTC_PLUS_3)


def parse_user_datetime(value: str) -> datetime:
    dt = datetime.strptime(value.strip(), "%d.%m.%Y %H:%M")
    return dt.replace(tzinfo=UTC_PLUS_3)


def format_dt(dt: Optional[datetime], with_seconds: bool = False) -> str:
    if not dt:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC_PLUS_3)
    else:
        dt = dt.astimezone(UTC_PLUS_3)
    return dt.strftime("%d.%m.%Y %H:%M:%S" if with_seconds else "%d.%m.%Y %H:%M")


def now_text() -> str:
    return format_dt(now_utc3())


def format_remaining_time(target_dt: Optional[datetime]) -> str:
    if not target_dt:
        return "—"

    delta = target_dt - now_utc3()
    total_seconds = int(delta.total_seconds())

    if total_seconds <= 0:
        return "меньше минуты"

    if total_seconds < 60:
        return f"{total_seconds}с"

    total_minutes = total_seconds // 60
    hours_total = total_minutes // 60
    minutes = total_minutes % 60

    if hours_total > 0:
        if minutes > 0:
            return f"{hours_total}ч {minutes}м"
        return f"{hours_total}ч"

    return f"{total_minutes}м"


# =========================
# SQLITE
# =========================
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db():
    with closing(get_conn()) as conn:
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
                created_at TEXT,
                post_message_id INTEGER,
                status_message_id INTEGER
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS participants (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                joined_at TEXT,
                chosen_number INTEGER UNIQUE
            )
        """)

        cur.execute("INSERT OR IGNORE INTO giveaway (id, active) VALUES (1, 0)")

        try:
            cur.execute("ALTER TABLE participants ADD COLUMN chosen_number INTEGER UNIQUE")
        except sqlite3.OperationalError:
            pass

        try:
            cur.execute("ALTER TABLE giveaway ADD COLUMN post_message_id INTEGER")
        except sqlite3.OperationalError:
            pass

        try:
            cur.execute("ALTER TABLE giveaway ADD COLUMN status_message_id INTEGER")
        except sqlite3.OperationalError:
            pass

        conn.commit()


def save_giveaway(data: dict):
    payload = {
        "active": 1 if data.get("active") else 0,
        "media_type": data.get("media_type"),
        "media_file_id": data.get("media_file_id"),
        "description": data.get("description"),
        "start_mode": data.get("start_mode"),
        "start_datetime": data.get("start_datetime"),
        "end_mode": data.get("end_mode"),
        "end_value": data.get("end_value"),
        "end_datetime": data.get("end_datetime"),
        "winners_count": data.get("winners_count"),
        "code": data.get("code"),
        "created_at": data.get("created_at"),
        "post_message_id": data.get("post_message_id"),
        "status_message_id": data.get("status_message_id"),
    }

    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE giveaway
            SET active = :active,
                media_type = :media_type,
                media_file_id = :media_file_id,
                description = :description,
                start_mode = :start_mode,
                start_datetime = :start_datetime,
                end_mode = :end_mode,
                end_value = :end_value,
                end_datetime = :end_datetime,
                winners_count = :winners_count,
                code = :code,
                created_at = :created_at,
                post_message_id = :post_message_id,
                status_message_id = :status_message_id
            WHERE id = 1
            """,
            payload,
        )
        conn.commit()


def load_giveaway() -> dict:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT active, media_type, media_file_id, description, start_mode,
                   start_datetime, end_mode, end_value, end_datetime,
                   winners_count, code, created_at, post_message_id, status_message_id
            FROM giveaway
            WHERE id = 1
            """
        )
        row = cur.fetchone()

    if not row:
        return {"active": False}

    return {
        "active": bool(row[0]),
        "media_type": row[1],
        "media_file_id": row[2],
        "description": row[3],
        "start_mode": row[4],
        "start_datetime": row[5],
        "end_mode": row[6],
        "end_value": row[7],
        "end_datetime": row[8],
        "winners_count": row[9],
        "code": row[10],
        "created_at": row[11],
        "post_message_id": row[12],
        "status_message_id": row[13],
    }


def clear_giveaway_db():
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
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
                created_at = NULL,
                post_message_id = NULL,
                status_message_id = NULL
            WHERE id = 1
            """
        )
        conn.commit()


def add_participant(user_id: int, username: str, chosen_number: int) -> bool:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO participants (user_id, username, joined_at, chosen_number)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, username, now_utc3().isoformat(), chosen_number),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def participant_exists(user_id: int) -> bool:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM participants WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row is not None


def get_participant_number(user_id: int) -> Optional[int]:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT chosen_number FROM participants WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


def number_is_taken(number: int) -> bool:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM participants WHERE chosen_number = ?", (number,))
        row = cur.fetchone()
        return row is not None


def get_current_number_max() -> int:
    if current_giveaway.get("end_mode") == "count":
        end_value = current_giveaway.get("end_value")
        if isinstance(end_value, int) and end_value > 0:
            return end_value
    return DEFAULT_NUMBER_MAX


def get_numbers_info_text() -> str:
    if current_giveaway.get("end_mode") == "time":
        return "автоназначение по 100"
    return f"{NUMBER_MIN}–{get_current_number_max()}"


def get_free_numbers() -> List[int]:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT chosen_number FROM participants WHERE chosen_number IS NOT NULL")
        taken_rows = cur.fetchall()

    taken = {row[0] for row in taken_rows if row[0] is not None}
    max_number = get_current_number_max()
    return [n for n in range(NUMBER_MIN, max_number + 1) if n not in taken]


def get_participants() -> List[Tuple[int, str, str, Optional[int]]]:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, username, joined_at, chosen_number
            FROM participants
            ORDER BY joined_at ASC
            """
        )
        return cur.fetchall()


def get_participants_count() -> int:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM participants")
        return cur.fetchone()[0]


def clear_participants():
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM participants")
        conn.commit()


# =========================
# HELPERS
# =========================
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def build_caption(description: str) -> str:
    return description


def current_time_hint() -> str:
    return f"{TIMEZONE_TEXT}\n🕒 Сейчас: {now_text()}"


def user_rate_limited(user_id: int) -> Optional[str]:
    now_ts = time.time()

    if user_id in wrong_code_attempts:
        blocked_until = wrong_code_attempts[user_id]["until"]
        if blocked_until > now_ts:
            minutes_left = int((blocked_until - now_ts) // 60) + 1
            return f"Слишком много неверных попыток. Попробуй через {minutes_left} мин."

    last_ts = last_message_time.get(user_id, 0)
    if now_ts - last_ts < MESSAGE_COOLDOWN_SECONDS:
        return "Слишком быстро. Подожди пару секунд."

    last_message_time[user_id] = now_ts
    return None


def register_wrong_code_attempt(user_id: int):
    now_ts = time.time()
    current = wrong_code_attempts.get(user_id, {"count": 0, "until": 0})
    current["count"] += 1

    if current["count"] >= MAX_WRONG_CODE_ATTEMPTS:
        current["until"] = now_ts + WRONG_CODE_BLOCK_MINUTES * 60
        current["count"] = 0

    wrong_code_attempts[user_id] = current


def reset_wrong_code_attempts(user_id: int):
    wrong_code_attempts.pop(user_id, None)


def reset_runtime_state():
    global current_giveaway
    current_giveaway = {"active": False}
    pending_number_choice.clear()


def admin_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🎁 Новый розыгрыш", callback_data="admin_new"),
        InlineKeyboardButton("📊 Статус", callback_data="admin_status"),
        InlineKeyboardButton("🧾 Список участников", callback_data="admin_list"),
        InlineKeyboardButton("❌ Отменить розыгрыш", callback_data="admin_cancel"),
    )
    return kb


def cancel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="setup_cancel"))
    return kb


def start_mode_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("⚡ Сейчас", callback_data="start_now"))
    kb.add(
        InlineKeyboardButton("⏱ +5 мин", callback_data="start_5"),
        InlineKeyboardButton("⏱ +15 мин", callback_data="start_15"),
        InlineKeyboardButton("⏱ +30 мин", callback_data="start_30"),
        InlineKeyboardButton("⏱ +1 час", callback_data="start_60"),
    )
    kb.add(InlineKeyboardButton("🕒 Выбрать время", callback_data="start_schedule"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="setup_cancel"))
    return kb


def end_mode_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👥 По количеству", callback_data="end_count"),
        InlineKeyboardButton("⏰ По времени", callback_data="end_time"),
    )
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="setup_cancel"))
    return kb


def publish_preview_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🚀 Опубликовать", callback_data="publish_now"),
        InlineKeyboardButton("❌ Отмена", callback_data="publish_cancel"),
    )
    return kb


def choose_number_kb() -> InlineKeyboardMarkup:
    free_numbers = get_free_numbers()
    kb = InlineKeyboardMarkup(row_width=1)
    if free_numbers:
        kb.add(InlineKeyboardButton("🎲 Случайный номер", callback_data="picknum_random"))
    return kb


async def send_step_hint(message: types.Message, text: str):
    await message.answer(
        f"{text}\n\nЕсли передумал — нажми «Отмена».",
        reply_markup=cancel_kb(),
    )


async def send_step_hint_cb(callback: types.CallbackQuery, text: str):
    await callback.message.answer(
        f"{text}\n\nЕсли передумал — нажми «Отмена».",
        reply_markup=cancel_kb(),
    )


async def safe_edit_message(chat_id: str, message_id: int, text: str) -> bool:
    if not message_id:
        return False

    for _ in range(3):
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
            return True
        except MessageNotModified:
            return True
        except RetryAfter as e:
            await asyncio.sleep(float(e.timeout) + 0.5)
        except Exception as e:
            if "message is not modified" in str(e).lower():
                return True
            if "retry in" in str(e).lower():
                wait_seconds = 2
                try:
                    parts = "".join(ch if ch.isdigit() else " " for ch in str(e)).split()
                    if parts:
                        wait_seconds = int(parts[0])
                except Exception:
                    pass
                await asyncio.sleep(wait_seconds + 0.5)
            else:
                logger.warning("Не удалось отредактировать сообщение %s в %s: %s", message_id, chat_id, e)
                return False

    return False


async def safe_delete_message(chat_id: str, message_id: Optional[int]):
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def notify_admin(text: str, reply_markup=None):
    try:
        await bot.send_message(ADMIN_ID, text, reply_markup=reply_markup)
    except Exception as e:
        logger.exception("Не удалось отправить сообщение админу: %s", e)


def build_status_text() -> str:
    if current_giveaway.get("active"):
        if current_giveaway.get("end_mode") == "count":
            total_needed = current_giveaway.get("end_value", 0) or 0
            current_count = get_participants_count()
            left = max(total_needed - current_count, 0)

            if left <= 0:
                return "🎉 Розыгрыш завершён!"
            if left == 1:
                return "👥 Осталось мест: 1\n\n🚨 Последний шанс!"
            if left == 2:
                return "👥 Осталось мест: 2\n\n⚡ Решающий момент!"
            if left == 3:
                return "👥 Осталось мест: 3\n\n🔥 Последние участники!"
            if left == 4:
                return "👥 Осталось мест: 4\n\n👀 Уже близко..."
            if left == 5:
                return "👥 Осталось мест: 5\n\n🔥 Почти финал!"
            if left <= 10:
                return f"👥 Осталось мест: {left}\n\n⚡ Быстро разбирают!"
            return f"👥 Осталось мест: {left}"

        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if not end_dt:
            return "⏳ Розыгрыш идёт"

        seconds_left = int((end_dt - now_utc3()).total_seconds())
        if seconds_left <= 0:
            return "🎉 Розыгрыш завершён!"
        if seconds_left == 5:
            return "⏳ Ещё 5 секунд…"
        if seconds_left <= 60:
            return "⏳ Менее одной минуты"

        return f"⏳ До конца розыгрыша: {format_remaining_time(end_dt)}"

    if current_giveaway.get("start_mode") == "scheduled":
        start_dt = parse_dt(current_giveaway.get("start_datetime"))
        if not start_dt:
            return "⏳ Розыгрыш скоро начнётся"

        seconds_left = int((start_dt - now_utc3()).total_seconds())
        if seconds_left <= 0:
            return "🚀 Розыгрыш запускается..."
        if seconds_left <= 60:
            return "⏳ Начало нового розыгрыша менее чем через минуту…"
        return f"⏳ Начало нового розыгрыша через {format_remaining_time(start_dt)}"

    return "ℹ️ Розыгрыш не активен"


async def publish_status_message():
    status_text = build_status_text()
    msg = await bot.send_message(CHANNEL_ID, status_text)
    current_giveaway["status_message_id"] = msg.message_id
    save_giveaway(current_giveaway)


async def update_status_message():
    async with status_lock:
        status_message_id = current_giveaway.get("status_message_id")
        if not status_message_id:
            return

        text = build_status_text()
        edited = await safe_edit_message(CHANNEL_ID, status_message_id, text)
        if not edited:
            msg = await bot.send_message(CHANNEL_ID, text)
            current_giveaway["status_message_id"] = msg.message_id
            save_giveaway(current_giveaway)


async def start_status_updates():
    try:
        scheduler.remove_job(STATUS_JOB_ID)
    except Exception:
        pass

    interval_seconds = 1
    if current_giveaway.get("start_mode") == "scheduled" and not current_giveaway.get("active"):
        interval_seconds = 1
    if current_giveaway.get("active") and current_giveaway.get("end_mode") == "count":
        interval_seconds = 2
    if current_giveaway.get("active") and current_giveaway.get("end_mode") == "time":
        interval_seconds = 1

    scheduler.add_job(
        update_status_message,
        trigger="interval",
        seconds=interval_seconds,
        id=STATUS_JOB_ID,
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )


async def stop_status_updates():
    try:
        scheduler.remove_job(STATUS_JOB_ID)
    except Exception:
        pass


async def publish_giveaway_post():
    media_type = current_giveaway.get("media_type")
    media_file_id = current_giveaway.get("media_file_id")
    description = current_giveaway.get("description")

    if not media_type or not media_file_id:
        raise RuntimeError("Не хватает медиа для публикации")

    if media_type == "video":
        post = await bot.send_video(
            CHANNEL_ID,
            media_file_id,
            caption=description,
            supports_streaming=False,
            protect_content=True,
        )
    else:
        post = await bot.send_photo(
            CHANNEL_ID,
            media_file_id,
            caption=description,
            protect_content=True,
        )

    current_giveaway["post_message_id"] = post.message_id
    save_giveaway(current_giveaway)


async def publish_active_status_under_post():
    old_status_id = current_giveaway.get("status_message_id")
    if old_status_id:
        await safe_delete_message(CHANNEL_ID, old_status_id)
        current_giveaway["status_message_id"] = None
        save_giveaway(current_giveaway)

    await publish_status_message()
    await start_status_updates()


async def notify_winner_in_private(user_id: int, place_text: str, chosen_number: Optional[int]) -> bool:
    number_text = f"\nТвой выигравший номер: {chosen_number}" if chosen_number is not None else ""
    try:
        await bot.send_message(
            user_id,
            f"🎉 Поздравляем!\n"
            f"Ты {place_text} в розыгрыше.{number_text}\n\n"
            f"Напиши администратору для получения приза.",
        )
        return True
    except Exception:
        return False


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


def get_time_mode_block_range(participant_index: int) -> Tuple[int, int]:
    block_index = (participant_index - 1) // 100
    start = block_index * 100 + 1
    end = start + 99
    return start, end


def get_taken_numbers_in_range(start: int, end: int) -> set:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT chosen_number
            FROM participants
            WHERE chosen_number BETWEEN ? AND ?
            """,
            (start, end),
        )
        rows = cur.fetchall()
    return {row[0] for row in rows if row[0] is not None}


async def assign_time_mode_number(user_id: int, display_name: str) -> Optional[int]:
    async with join_lock:
        for _ in range(10):
            participant_index = get_participants_count() + 1
            start, end = get_time_mode_block_range(participant_index)
            taken = get_taken_numbers_in_range(start, end)
            free_numbers = [n for n in range(start, end + 1) if n not in taken]

            if not free_numbers:
                continue

            chosen_number = random.choice(free_numbers)
            ok = add_participant(user_id, display_name, chosen_number)
            if ok:
                return chosen_number

            await asyncio.sleep(0.05)

    return None


async def ask_for_number_choice(target, resend: bool = False):
    free_numbers = get_free_numbers()
    max_number = get_current_number_max()

    if not free_numbers:
        text = "Свободных номеров не осталось ❌"
        if isinstance(target, types.CallbackQuery):
            await target.message.answer(text)
        else:
            await target.answer(text)
        return

    prefix = "⚠️ Выбери другой номер.\n\n" if resend else "Код принят ✅\n\n"
    base_text = (
        f"{prefix}"
        f"🎯 Выбери свой счастливый номер от {NUMBER_MIN} до {max_number}\n"
        f"✍️ Напиши число в чат\n"
        f"🎲 Или нажми «Случайный номер»"
    )

    if isinstance(target, types.CallbackQuery):
        await target.message.answer(base_text, reply_markup=choose_number_kb())
    else:
        await target.answer(base_text, reply_markup=choose_number_kb())


# =========================
# GIVEAWAY CORE
# =========================
async def cleanup_current_giveaway_messages(delete_post: bool = True):
    if current_giveaway.get("status_message_id"):
        await safe_delete_message(CHANNEL_ID, current_giveaway.get("status_message_id"))
    if delete_post and current_giveaway.get("post_message_id"):
        await safe_delete_message(CHANNEL_ID, current_giveaway.get("post_message_id"))


async def cancel_current_giveaway(reason_text: str):
    async with lifecycle_lock:
        await remove_start_job()
        await remove_finish_job()
        await stop_status_updates()
        await cleanup_current_giveaway_messages(delete_post=True)
        clear_participants()
        clear_giveaway_db()
        reset_runtime_state()
        await notify_admin(reason_text, reply_markup=admin_menu_kb())


async def maybe_finish_count_mode():
    if not current_giveaway.get("active"):
        return
    if current_giveaway.get("end_mode") != "count":
        return

    target = current_giveaway.get("end_value") or 0
    if target > 0 and get_participants_count() >= target:
        await finish_giveaway("набрано нужное количество участников")
    else:
        await update_status_message()


async def finish_giveaway(reason: str):
    global current_giveaway

    async with lifecycle_lock:
        if not current_giveaway.get("active"):
            return

        participants = get_participants()
        winners_count = current_giveaway.get("winners_count", 1) or 1

        await remove_finish_job()
        await stop_status_updates()

        if not participants:
            empty_text = "🏁 Розыгрыш завершён\n\nУчастников нет ❌"
            edited = False

            if current_giveaway.get("status_message_id"):
                edited = await safe_edit_message(
                    CHANNEL_ID,
                    current_giveaway["status_message_id"],
                    empty_text,
                )

            if not edited:
                msg = await bot.send_message(CHANNEL_ID, empty_text)
                current_giveaway["status_message_id"] = msg.message_id
                save_giveaway(current_giveaway)

            await notify_admin(
                f"Розыгрыш завершён ({reason}), но участников нет ❌",
                reply_markup=admin_menu_kb(),
            )
        else:
            actual_winners_count = min(winners_count, len(participants))
            winner_rows = random.sample(participants, actual_winners_count)

            medals = ["🥇", "🥈", "🥉"]
            winners_lines = []

            for i, (_, winner_name, _, chosen_number) in enumerate(winner_rows, start=1):
                medal = medals[i - 1] if i <= len(medals) else f"{i}."
                number_text = f" — №{chosen_number}" if chosen_number is not None else ""
                winners_lines.append(f"{medal} {winner_name}{number_text}")

            winners_text = "\n".join(winners_lines)

            if len(winner_rows) == 1:
                _, winner_name, _, chosen_number = winner_rows[0]
                result_text = (
                    f"🎉 ПОБЕДИТЕЛЬ ОПРЕДЕЛЁН!\n\n"
                    f"🏆 {winner_name}\n"
                    f"🎯 Номер: {chosen_number}"
                )
            else:
                result_text = f"🎉 ПОБЕДИТЕЛИ ОПРЕДЕЛЕНЫ!\n\n🏆 Итоги розыгрыша:\n{winners_text}"

            edited = False
            if current_giveaway.get("status_message_id"):
                edited = await safe_edit_message(
                    CHANNEL_ID,
                    current_giveaway["status_message_id"],
                    result_text,
                )

            if not edited:
                msg = await bot.send_message(CHANNEL_ID, result_text)
                current_giveaway["status_message_id"] = msg.message_id
                save_giveaway(current_giveaway)

            await notify_admin(
                f"🎉 Розыгрыш завершён!\n\n"
                f"Причина: {reason}\n"
                f"Участников: {len(participants)}\n"
                f"Победителей: {actual_winners_count}\n\n"
                f"{winners_text}",
                reply_markup=admin_menu_kb(),
            )

            for i, (winner_id, _, _, chosen_number) in enumerate(winner_rows, start=1):
                place_text = "победитель" if i == 1 else f"в числе победителей (место {i})"
                await notify_winner_in_private(winner_id, place_text, chosen_number)

        clear_participants()
        clear_giveaway_db()
        reset_runtime_state()


async def start_giveaway(clear_existing_participants: bool = True):
    global finish_job_id

    async with lifecycle_lock:
        if clear_existing_participants:
            clear_participants()
        pending_number_choice.clear()

        current_giveaway["active"] = True
        save_giveaway(current_giveaway)

        try:
            if not current_giveaway.get("post_message_id"):
                await publish_giveaway_post()
            await publish_active_status_under_post()
        except Exception as e:
            logger.exception("Ошибка публикации розыгрыша: %s", e)
            current_giveaway["active"] = False
            save_giveaway(current_giveaway)
            await notify_admin(f"Ошибка публикации розыгрыша: {e}", reply_markup=admin_menu_kb())
            return

        if current_giveaway.get("end_mode") == "time":
            end_dt = parse_dt(current_giveaway.get("end_datetime"))
            if end_dt:
                finish_job_id = f"finish_{int(time.time())}"
                scheduler.add_job(
                    finish_giveaway,
                    trigger="date",
                    run_date=end_dt,
                    args=["вышло время"],
                    id=finish_job_id,
                    replace_existing=True,
                    misfire_grace_time=5,
                    coalesce=True,
                    max_instances=1,
                )


async def scheduled_start():
    await start_giveaway(clear_existing_participants=True)


async def restore_state():
    global current_giveaway, finish_job_id, start_job_id

    current_giveaway = load_giveaway() or {"active": False}

    if current_giveaway.get("active"):
        try:
            if not current_giveaway.get("post_message_id"):
                await publish_giveaway_post()

            if not current_giveaway.get("status_message_id"):
                await publish_status_message()

            await start_status_updates()
            await update_status_message()

            if current_giveaway.get("end_mode") == "time":
                end_dt = parse_dt(current_giveaway.get("end_datetime"))
                if end_dt:
                    if now_utc3() >= end_dt:
                        await finish_giveaway("вышло время после перезапуска")
                    else:
                        finish_job_id = FINISH_JOB_ID_RESTORED
                        scheduler.add_job(
                            finish_giveaway,
                            trigger="date",
                            run_date=end_dt,
                            args=["вышло время"],
                            id=finish_job_id,
                            replace_existing=True,
                            misfire_grace_time=5,
                            coalesce=True,
                            max_instances=1,
                        )
        except Exception as e:
            logger.exception("Ошибка восстановления активного розыгрыша: %s", e)
            await notify_admin(f"Ошибка восстановления активного розыгрыша: {e}")
        return

    if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
        try:
            if current_giveaway.get("post_message_id"):
                await safe_delete_message(CHANNEL_ID, current_giveaway.get("post_message_id"))
                current_giveaway["post_message_id"] = None
                save_giveaway(current_giveaway)

            if not current_giveaway.get("status_message_id"):
                await publish_status_message()
            else:
                await update_status_message()

            await start_status_updates()

            start_dt = parse_dt(current_giveaway.get("start_datetime"))
            if start_dt:
                if now_utc3() >= start_dt:
                    await start_giveaway(clear_existing_participants=True)
                else:
                    start_job_id = START_JOB_ID_RESTORED
                    scheduler.add_job(
                        scheduled_start,
                        trigger="date",
                        run_date=start_dt,
                        id=start_job_id,
                        replace_existing=True,
                        misfire_grace_time=5,
                        coalesce=True,
                        max_instances=1,
                    )
        except Exception as e:
            logger.exception("Ошибка восстановления запланированного розыгрыша: %s", e)
            await notify_admin(f"Ошибка восстановления запланированного розыгрыша: {e}")


async def on_startup(_):
    await bot.delete_webhook(drop_pending_updates=True)
    init_db()
    scheduler.start()
    await restore_state()
    await notify_admin("Бот запущен ✅\n\nВыбери действие:", reply_markup=admin_menu_kb())


# =========================
# ADMIN COMMANDS
# =========================
@dp.message_handler(commands=["admin"], state="*")
async def admin_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await state.finish()
    await message.answer("⚙️ Админ-меню", reply_markup=admin_menu_kb())


@dp.message_handler(commands=["cancelsetup"], state="*")
async def cancel_setup_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await state.finish()
    await remove_start_job()
    await remove_finish_job()
    await stop_status_updates()
    clear_giveaway_db()
    reset_runtime_state()
    await message.answer("Создание розыгрыша отменено ❌", reply_markup=admin_menu_kb())


@dp.message_handler(commands=["cancel"], state="*")
async def cancel_cmd(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await state.finish()
    await cancel_current_giveaway("Розыгрыш отменён ❌")


# =========================
# USER COMMANDS
# =========================
@dp.message_handler(commands=["start"], state="*")
async def start_cmd(message: types.Message):
    if not current_giveaway.get("active"):
        await message.answer(
            "Сейчас активного розыгрыша нет ❌\n\n"
            "Когда начнётся новый розыгрыш, здесь появится инструкция для участия."
        )
        return

    if current_giveaway.get("end_mode") == "time":
        await message.answer(
            "Привет 👋\n\n"
            "Чтобы участвовать:\n"
            "1. Подпишись на канал\n"
            "2. Найди код\n"
            "3. Отправь код сюда\n\n"
            "🎯 Номер бот назначит автоматически\n"
            "Первые 100 участников получают случайные номера от 1 до 100,\n"
            "следующие 100 — от 101 до 200 и так далее."
        )
        return

    max_number = get_current_number_max()
    await message.answer(
        "Привет 👋\n\n"
        "Чтобы участвовать:\n"
        "1. Подпишись на канал\n"
        "2. Найди код\n"
        "3. Отправь код сюда\n"
        f"4. Выбери свой счастливый номер от {NUMBER_MIN} до {max_number}\n\n"
        "✍️ Напиши число в чат\n"
        "🎲 Или нажми «Случайный номер»"
    )


@dp.message_handler(commands=["ikota"], state="*")
async def ikota_cmd(message: types.Message):
    if is_admin(message.from_user.id):
        await message.answer(
            "Команды участника:\n"
            "/start — информация о текущем розыгрыше\n"
            "/ikota — список доступных команд\n\n"
            "Команды админа:\n"
            "/admin — открыть админ-меню\n"
            "/status — статус активного или запланированного розыгрыша\n"
            "/cancelsetup — отменить создание розыгрыша\n"
            "/cancel — отменить активный или запланированный розыгрыш"
        )
    else:
        await message.answer(
            "Доступные команды:\n"
            "/start — информация о текущем розыгрыше\n"
            "/ikota — список доступных команд"
        )


@dp.message_handler(commands=["status"], state="*")
async def status_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    if not current_giveaway.get("active"):
        if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
            start_dt = parse_dt(current_giveaway.get("start_datetime"))
            finish_text = (
                f"по количеству: {current_giveaway.get('end_value')}"
                if current_giveaway.get("end_mode") == "count"
                else format_dt(parse_dt(current_giveaway.get("end_datetime")))
            )

            await message.answer(
                f"📅 Запланированный розыгрыш\n\n"
                f"Старт: через {format_remaining_time(start_dt)}\n"
                f"Завершение: {finish_text}\n"
                f"Победителей: {current_giveaway.get('winners_count')}\n"
                f"Номера: {get_numbers_info_text()}\n"
                f"{TIMEZONE_TEXT}",
                reply_markup=admin_menu_kb(),
            )
            return

        await message.answer("Сейчас активного розыгрыша нет", reply_markup=admin_menu_kb())
        return

    text = (
        f"📊 Статус розыгрыша\n\n"
        f"Код: {current_giveaway.get('code')}\n"
        f"Участников: {get_participants_count()}\n"
        f"Победителей: {current_giveaway.get('winners_count')}\n"
        f"Номера: {get_numbers_info_text()}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Завершение: по количеству\nЦель: {current_giveaway.get('end_value')}"
    else:
        text += f"Завершение: по времени\nДо: {format_dt(parse_dt(current_giveaway.get('end_datetime')))}\n{TIMEZONE_TEXT}"

    await message.answer(text, reply_markup=admin_menu_kb())


# =========================
# ADMIN CALLBACKS
# =========================
@dp.callback_query_handler(lambda c: c.data == "admin_new")
async def cb_admin_new(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    if current_giveaway.get("active"):
        await callback.message.answer("Сейчас уже есть активный розыгрыш ❗")
        await callback.answer()
        return

    if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
        await callback.message.answer("Сейчас уже есть запланированный розыгрыш ❗")
        await callback.answer()
        return

    await GiveawayForm.waiting_media.set()
    await send_step_hint_cb(
        callback,
        "Отправь фото или видео для поста.\n\nМожно прислать как медиа или как файл.",
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_status")
async def cb_admin_status(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    if not current_giveaway.get("active"):
        if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
            start_dt = parse_dt(current_giveaway.get("start_datetime"))
            finish_text = (
                f"по количеству: {current_giveaway.get('end_value')}"
                if current_giveaway.get("end_mode") == "count"
                else format_dt(parse_dt(current_giveaway.get("end_datetime")))
            )

            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("❌ Отменить", callback_data="admin_cancel"))

            await callback.message.answer(
                f"📅 Запланированный розыгрыш\n\n"
                f"Код: {current_giveaway.get('code')}\n"
                f"Старт: через {format_remaining_time(start_dt)}\n"
                f"Завершение: {finish_text}\n"
                f"Победителей: {current_giveaway.get('winners_count')}\n"
                f"Номера: {get_numbers_info_text()}\n"
                f"{TIMEZONE_TEXT}",
                reply_markup=kb,
            )
            await callback.answer()
            return

        await callback.message.answer("Сейчас активного розыгрыша нет")
        await callback.answer()
        return

    text = (
        f"📊 Статус розыгрыша\n\n"
        f"Код: {current_giveaway.get('code')}\n"
        f"Участников: {get_participants_count()}\n"
        f"Победителей: {current_giveaway.get('winners_count')}\n"
        f"Номера: {get_numbers_info_text()}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Завершение: по количеству\nЦель: {current_giveaway.get('end_value')}"
    else:
        text += f"Завершение: по времени\nДо: {format_dt(parse_dt(current_giveaway.get('end_datetime')))}\n{TIMEZONE_TEXT}"

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("🧾 Участники", callback_data="admin_list"),
        InlineKeyboardButton("🛑 Завершить сейчас", callback_data="admin_finish_now"),
    )
    kb.add(InlineKeyboardButton("❌ Отменить", callback_data="admin_cancel"))

    await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_finish_now")
async def cb_finish_now(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    await finish_giveaway("завершено вручную")
    await callback.answer("Розыгрыш завершён")


@dp.callback_query_handler(lambda c: c.data == "admin_list")
async def cb_admin_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    participants = get_participants()
    if not participants:
        await callback.message.answer("Участников пока нет")
        await callback.answer()
        return

    lines = ["🧾 Список участников:\n"]
    for i, (_, username, joined_at, chosen_number) in enumerate(participants[:50], start=1):
        number_text = f"№{chosen_number}" if chosen_number is not None else "без номера"
        lines.append(f"{i}. {username} — {number_text} — {format_dt(parse_dt(joined_at))}")

    if len(participants) > 50:
        lines.append(f"\nИ ещё {len(participants) - 50} участников...")

    await callback.message.answer("\n".join(lines))
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_cancel")
async def cb_admin_cancel(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    await cancel_current_giveaway("Активный или запланированный розыгрыш отменён ❌")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "setup_cancel", state="*")
async def cb_setup_cancel(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    await state.finish()
    await remove_start_job()
    await remove_finish_job()
    await stop_status_updates()
    clear_giveaway_db()
    reset_runtime_state()
    await callback.message.answer("Создание розыгрыша отменено ❌", reply_markup=admin_menu_kb())
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "publish_now")
async def cb_publish_now(callback: types.CallbackQuery):
    global start_job_id

    if not is_admin(callback.from_user.id):
        return

    if not current_giveaway:
        await callback.answer("Нет данных для публикации", show_alert=True)
        return

    if not current_giveaway.get("media_type") or not current_giveaway.get("media_file_id") or not current_giveaway.get("code"):
        await callback.answer("Черновик уже неактуален", show_alert=True)
        return

    if current_giveaway.get("start_mode") == "now":
        await start_giveaway(clear_existing_participants=True)
        await callback.message.answer("✅ Розыгрыш опубликован", reply_markup=admin_menu_kb())
        await callback.answer()
        return

    await remove_start_job()
    start_dt = parse_dt(current_giveaway.get("start_datetime"))
    if not start_dt:
        await callback.message.answer("Ошибка: время старта не задано", reply_markup=admin_menu_kb())
        await callback.answer()
        return

    if start_dt <= now_utc3():
        await callback.message.answer("Ошибка: время старта уже прошло", reply_markup=admin_menu_kb())
        await callback.answer()
        return

    try:
        if not current_giveaway.get("status_message_id"):
            await publish_status_message()
        else:
            await update_status_message()

        await start_status_updates()

        start_job_id = f"start_{int(time.time())}"
        scheduler.add_job(
            scheduled_start,
            trigger="date",
            run_date=start_dt,
            id=start_job_id,
            replace_existing=True,
            misfire_grace_time=5,
            coalesce=True,
            max_instances=1,
        )

        await callback.message.answer(
            f"✅ Розыгрыш запланирован\nСтарт через {format_remaining_time(start_dt)}",
            reply_markup=admin_menu_kb(),
        )
    except Exception as e:
        logger.exception("Ошибка планирования розыгрыша: %s", e)
        await callback.message.answer(f"Ошибка планирования: {e}", reply_markup=admin_menu_kb())

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "publish_cancel")
async def cb_publish_cancel(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    await remove_start_job()
    await remove_finish_job()
    await stop_status_updates()

    if current_giveaway.get("status_message_id"):
        await safe_delete_message(CHANNEL_ID, current_giveaway.get("status_message_id"))

    clear_giveaway_db()
    reset_runtime_state()

    await callback.message.answer("Публикация отменена ❌", reply_markup=admin_menu_kb())
    await callback.answer()


# =========================
# ВЫБОР НОМЕРА ЧЕРЕЗ КНОПКУ
# =========================
@dp.callback_query_handler(lambda c: c.data.startswith("picknum_"), state=NumberChoiceForm.waiting_number)
async def cb_pick_number(callback: types.CallbackQuery, state: FSMContext):
    if not current_giveaway.get("active"):
        pending_number_choice.discard(callback.from_user.id)
        await state.finish()
        await callback.message.answer("Сейчас нет активного розыгрыша")
        await callback.answer()
        return

    user_id = callback.from_user.id

    if user_id not in pending_number_choice:
        await state.finish()
        await callback.answer("Сначала отправь код для участия", show_alert=True)
        return

    async with join_lock:
        free_numbers = get_free_numbers()
        if not free_numbers:
            pending_number_choice.discard(user_id)
            await state.finish()
            await callback.message.answer("Свободных номеров не осталось ❌")
            await callback.answer()
            return

        chosen_number = random.choice(free_numbers)

        username = callback.from_user.username
        full_name = callback.from_user.full_name
        display_name = f"@{username}" if username else full_name

        ok = add_participant(user_id, display_name, chosen_number)
        if not ok:
            await callback.answer("Этот номер только что заняли", show_alert=True)
            await ask_for_number_choice(callback, resend=True)
            return

    pending_number_choice.discard(user_id)
    reset_wrong_code_attempts(user_id)
    await state.finish()

    await callback.message.answer(
        f"Готово ✅\n"
        f"Ты участвуешь в розыгрыше\n"
        f"Твой номер: {chosen_number}"
    )
    await callback.answer("Номер закреплён")

    await maybe_finish_count_mode()


# =========================
# FSM: ОСНОВНОЙ FLOW
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
        name = (message.document.file_name or "").lower()
        if name.endswith((".mp4", ".mov", ".mkv")):
            media_type = "video"
        elif name.endswith((".jpg", ".jpeg", ".png", ".webp")):
            media_type = "photo"
        else:
            await send_step_hint(message, "Подходит только фото или видео.\n\nОтправь другой файл.")
            return
        file_id = message.document.file_id
    else:
        await send_step_hint(message, "Отправь фото или видео.")
        return

    await state.update_data(media_type=media_type, media_file_id=file_id)
    await GiveawayForm.waiting_description.set()
    await send_step_hint(message, "Теперь отправь описание поста")


@dp.message_handler(state=GiveawayForm.waiting_description, content_types=types.ContentTypes.TEXT)
async def process_description(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text:
        await send_step_hint(message, "Описание не должно быть пустым. Отправь текст поста.")
        return

    await state.update_data(description=text)
    await GiveawayForm.waiting_start_mode.set()
    await message.answer("Когда запускать розыгрыш?", reply_markup=start_mode_kb())


@dp.callback_query_handler(
    lambda c: c.data in ["start_now", "start_schedule", "start_5", "start_15", "start_30", "start_60"],
    state=GiveawayForm.waiting_start_mode,
)
async def process_start_mode(callback: types.CallbackQuery, state: FSMContext):
    now = now_utc3()

    if callback.data == "start_now":
        await state.update_data(start_mode="now", start_datetime=None)
        await GiveawayForm.waiting_end_mode.set()
        await callback.message.answer("Как завершать розыгрыш?", reply_markup=end_mode_kb())
    elif callback.data == "start_schedule":
        await state.update_data(start_mode="scheduled")
        await GiveawayForm.waiting_start_datetime.set()
        await send_step_hint_cb(
            callback,
            f"Введи дату и время старта в формате:\n01.01.2000 23:59\n\n{current_time_hint()}",
        )
    else:
        if callback.data == "start_5":
            start_dt = now + timedelta(minutes=5)
        elif callback.data == "start_15":
            start_dt = now + timedelta(minutes=15)
        elif callback.data == "start_30":
            start_dt = now + timedelta(minutes=30)
        else:
            start_dt = now + timedelta(hours=1)

        await state.update_data(start_mode="scheduled", start_datetime=start_dt.isoformat())
        await GiveawayForm.waiting_end_mode.set()
        await callback.message.answer(
            f"✅ Старт установлен\nЧерез {format_remaining_time(start_dt)}\n\nКак завершать розыгрыш?",
            reply_markup=end_mode_kb(),
        )

    await callback.answer()


@dp.message_handler(state=GiveawayForm.waiting_start_datetime, content_types=types.ContentTypes.TEXT)
async def process_start_datetime(message: types.Message, state: FSMContext):
    try:
        start_dt = parse_user_datetime(message.text)
    except ValueError:
        await send_step_hint(message, f"Неверный формат.\nПример: 01.01.2000 23:59\n\n{current_time_hint()}")
        return

    if start_dt <= now_utc3():
        await send_step_hint(message, f"Время старта должно быть в будущем.\n\n{current_time_hint()}")
        return

    await state.update_data(start_datetime=start_dt.isoformat())
    await GiveawayForm.waiting_end_mode.set()
    await message.answer("Как завершать розыгрыш?", reply_markup=end_mode_kb())


@dp.callback_query_handler(lambda c: c.data in ["end_count", "end_time"], state=GiveawayForm.waiting_end_mode)
async def process_end_mode(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "end_count":
        await state.update_data(end_mode="count", end_datetime=None)
        await GiveawayForm.waiting_end_count.set()
        await send_step_hint_cb(callback, "Сколько участников должно быть до завершения?")
    else:
        await state.update_data(end_mode="time", end_value=None)
        await GiveawayForm.waiting_end_datetime.set()
        await send_step_hint_cb(
            callback,
            f"Введи дату и время завершения в формате:\n01.01.2000 23:59\n\n{current_time_hint()}",
        )
    await callback.answer()


@dp.message_handler(state=GiveawayForm.waiting_end_count, content_types=types.ContentTypes.TEXT)
async def process_end_count(message: types.Message, state: FSMContext):
    try:
        value = int(message.text.strip())
    except ValueError:
        await send_step_hint(message, "Нужно ввести число.")
        return

    if value <= 0:
        await send_step_hint(message, "Число должно быть больше нуля.")
        return

    await state.update_data(end_value=value, end_datetime=None)
    await GiveawayForm.waiting_winners_count.set()
    await send_step_hint(message, "Сколько победителей выбрать?")


@dp.message_handler(state=GiveawayForm.waiting_end_datetime, content_types=types.ContentTypes.TEXT)
async def process_end_datetime(message: types.Message, state: FSMContext):
    try:
        end_dt = parse_user_datetime(message.text)
    except ValueError:
        await send_step_hint(message, f"Неверный формат.\nПример: 01.01.2000 23:59\n\n{current_time_hint()}")
        return

    data = await state.get_data()
    start_mode = data.get("start_mode")
    start_datetime = data.get("start_datetime")

    if end_dt <= now_utc3():
        await send_step_hint(message, f"Время завершения должно быть в будущем.\n\n{current_time_hint()}")
        return

    if start_mode == "scheduled" and start_datetime:
        start_dt = parse_dt(start_datetime)
        if start_dt and end_dt <= start_dt:
            await send_step_hint(message, f"Время завершения должно быть позже времени старта.\n\n{current_time_hint()}")
            return

    await state.update_data(end_datetime=end_dt.isoformat(), end_value=None)
    await GiveawayForm.waiting_winners_count.set()
    await send_step_hint(message, "Сколько победителей выбрать?")


@dp.message_handler(state=GiveawayForm.waiting_winners_count, content_types=types.ContentTypes.TEXT)
async def process_winners_count(message: types.Message, state: FSMContext):
    try:
        winners_count = int(message.text.strip())
    except ValueError:
        await send_step_hint(message, "Нужно ввести число.")
        return

    if winners_count <= 0:
        await send_step_hint(message, "Количество победителей должно быть больше нуля.")
        return

    await state.update_data(winners_count=winners_count)
    await GiveawayForm.waiting_code.set()
    await send_step_hint(message, "Теперь отправь код, который участники будут вводить в бота")


@dp.message_handler(state=GiveawayForm.waiting_code, content_types=types.ContentTypes.TEXT)
async def process_code(message: types.Message, state: FSMContext):
    global current_giveaway

    code = (message.text or "").strip()
    if not code:
        await send_step_hint(message, "Код не должен быть пустым.")
        return

    await state.update_data(code=code)
    data = await state.get_data()

    current_giveaway = {
        "active": False,
        "media_type": data["media_type"],
        "media_file_id": data["media_file_id"],
        "description": data["description"],
        "start_mode": data["start_mode"],
        "start_datetime": data.get("start_datetime"),
        "end_mode": data["end_mode"],
        "end_value": data.get("end_value"),
        "end_datetime": data.get("end_datetime"),
        "winners_count": data["winners_count"],
        "code": data["code"],
        "created_at": now_utc3().isoformat(),
        "post_message_id": None,
        "status_message_id": None,
    }

    save_giveaway(current_giveaway)

    caption = build_caption(current_giveaway["description"])
    if current_giveaway["media_type"] == "video":
        await bot.send_video(
            ADMIN_ID,
            current_giveaway["media_file_id"],
            caption=caption,
            supports_streaming=False,
            protect_content=True,
        )
    else:
        await bot.send_photo(
            ADMIN_ID,
            current_giveaway["media_file_id"],
            caption=caption,
            protect_content=True,
        )

    await message.answer(
        "👇 Так будет выглядеть пост\n\nЕсли всё ок — нажми «Опубликовать».",
        reply_markup=publish_preview_kb(),
    )

    await state.finish()


# =========================
# FSM: FALLBACK ДЛЯ АДМИНА
# =========================
@dp.message_handler(state=GiveawayForm.waiting_media)
async def fallback_waiting_media(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "Сейчас бот ждёт фото или видео для поста.")


@dp.message_handler(state=GiveawayForm.waiting_description)
async def fallback_waiting_description(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "Сейчас бот ждёт описание поста текстом.")


@dp.message_handler(state=GiveawayForm.waiting_start_mode)
async def fallback_waiting_start_mode(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "Сейчас выбери способ старта кнопками ниже.\n\nЕсли передумал — нажми «Отмена».",
        reply_markup=start_mode_kb(),
    )


@dp.message_handler(state=GiveawayForm.waiting_start_datetime)
async def fallback_waiting_start_datetime(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, f"Сейчас бот ждёт дату и время старта в формате:\n01.01.2000 23:59\n\n{current_time_hint()}")


@dp.message_handler(state=GiveawayForm.waiting_end_mode)
async def fallback_waiting_end_mode(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "Сейчас выбери способ завершения кнопками ниже.\n\nЕсли передумал — нажми «Отмена».",
        reply_markup=end_mode_kb(),
    )


@dp.message_handler(state=GiveawayForm.waiting_end_count)
async def fallback_waiting_end_count(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "Сейчас бот ждёт количество участников для завершения розыгрыша.")


@dp.message_handler(state=GiveawayForm.waiting_end_datetime)
async def fallback_waiting_end_datetime(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, f"Сейчас бот ждёт дату и время завершения в формате:\n01.01.2000 23:59\n\n{current_time_hint()}")


@dp.message_handler(state=GiveawayForm.waiting_winners_count)
async def fallback_waiting_winners_count(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "Сейчас бот ждёт количество победителей.")


@dp.message_handler(state=GiveawayForm.waiting_code)
async def fallback_waiting_code(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(message, "Сейчас бот ждёт код для участников.")


# =========================
# ВЫБОР ПЕРСОНАЛЬНОГО НОМЕРА ТЕКСТОМ
# =========================
@dp.message_handler(state=NumberChoiceForm.waiting_number, content_types=types.ContentTypes.TEXT)
async def process_personal_number(message: types.Message, state: FSMContext):
    if not current_giveaway.get("active"):
        pending_number_choice.discard(message.from_user.id)
        await state.finish()
        await message.answer("Сейчас нет активного розыгрыша")
        return

    user_id = message.from_user.id

    if user_id not in pending_number_choice:
        await state.finish()
        await message.answer("Сначала отправь код для участия. Если бот перезапускался — отправь код ещё раз.")
        return

    max_number = get_current_number_max()

    try:
        chosen_number = int((message.text or "").strip())
    except ValueError:
        await message.answer(
            f"❌ Напиши число от {NUMBER_MIN} до {max_number}\n"
            f"🎲 Или нажми «Случайный номер»",
            reply_markup=choose_number_kb(),
        )
        return

    if chosen_number < NUMBER_MIN or chosen_number > max_number:
        await message.answer(
            f"❌ Можно выбрать число только от {NUMBER_MIN} до {max_number}",
            reply_markup=choose_number_kb(),
        )
        return

    async with join_lock:
        if number_is_taken(chosen_number):
            free_numbers = get_free_numbers()

            if not free_numbers:
                await message.answer("Свободных номеров не осталось ❌")
                pending_number_choice.discard(user_id)
                await state.finish()
                return

            if len(free_numbers) <= 10:
                hint = ", ".join(map(str, free_numbers))
                await message.answer(
                    f"⚠️ Номер {chosen_number} уже занят\n\n"
                    f"Свободные числа:\n{hint}\n\n"
                    f"🎲 Или нажми «Случайный номер»",
                    reply_markup=choose_number_kb(),
                )
            else:
                await message.answer(
                    f"⚠️ Номер {chosen_number} уже занят\n"
                    f"✍️ Напиши другой номер\n"
                    f"🎲 Или нажми «Случайный номер»",
                    reply_markup=choose_number_kb(),
                )
            return

        username = message.from_user.username
        full_name = message.from_user.full_name
        display_name = f"@{username}" if username else full_name

        ok = add_participant(user_id, display_name, chosen_number)
        if not ok:
            await message.answer(
                "Этот номер только что заняли ❌\nВыбери другой.",
                reply_markup=choose_number_kb(),
            )
            return

    pending_number_choice.discard(user_id)
    reset_wrong_code_attempts(user_id)
    await state.finish()

    await message.answer(
        f"Готово ✅\n"
        f"Ты участвуешь в розыгрыше\n"
        f"Твой номер: {chosen_number}"
    )

    await maybe_finish_count_mode()


# =========================
# УЧАСТИЕ ПОЛЬЗОВАТЕЛЯ
# =========================
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def check_code(message: types.Message):
    if not current_giveaway.get("active"):
        await message.answer("Сейчас нет активного розыгрыша")
        return

    user_id = message.from_user.id

    limiter_message = user_rate_limited(user_id)
    if limiter_message:
        await message.answer(limiter_message)
        return

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
    except Exception:
        await message.answer("Ошибка проверки подписки 😕")
        return

    if member.status not in ["member", "creator", "administrator"]:
        await message.answer("Сначала подпишись на канал ❗")
        return

    text = (message.text or "").strip()
    current_code = (current_giveaway.get("code") or "").strip()

    if text != current_code:
        register_wrong_code_attempt(user_id)
        await message.answer("Неверный код ❌\nПроверь внимательно и попробуй ещё раз")
        return

    if participant_exists(user_id):
        chosen_number = get_participant_number(user_id)
        number_text = f"\nТвой номер: {chosen_number}" if chosen_number is not None else ""
        await message.answer(f"Ты уже участвуешь в этом розыгрыше 🎉{number_text}")
        return

    username = message.from_user.username
    full_name = message.from_user.full_name
    display_name = f"@{username}" if username else full_name

    if current_giveaway.get("end_mode") == "time":
        chosen_number = await assign_time_mode_number(user_id, display_name)
        if chosen_number is None:
            await message.answer("Не удалось назначить номер 😕 Попробуй ещё раз.")
            return

        reset_wrong_code_attempts(user_id)
        await message.answer(
            "Код принят ✅\n\n"
            "🎯 Ты участвуешь в розыгрыше\n"
            f"Твой номер: {chosen_number}"
        )

        await update_status_message()
        return

    pending_number_choice.add(user_id)
    await NumberChoiceForm.waiting_number.set()
    await ask_for_number_choice(message)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)