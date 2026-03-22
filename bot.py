import os
import time
import random
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

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

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler(timezone=UTC_PLUS_3)

print("БОТ ЗАПУЩЕН ✅")


# =========================
# ВРЕМЕННОЕ СОСТОЯНИЕ
# =========================
last_message_time = {}
wrong_code_attempts = {}
pending_number_choice = set()

current_giveaway = {}
finish_job_id = None
start_job_id = None


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

    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    parts = []
    if days > 0:
        parts.append(f"{days}д")
    if hours > 0:
        parts.append(f"{hours}ч")
    if minutes > 0:
        parts.append(f"{minutes}м")
    if not parts and seconds > 0:
        parts.append(f"{seconds}с")

    return " ".join(parts) if parts else "меньше минуты"


# =========================
# SQLITE
# =========================
def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
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
    conn.close()


def save_giveaway(data: dict):
    conn = get_conn()
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
            created_at = ?,
            post_message_id = ?,
            status_message_id = ?
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
        data.get("post_message_id"),
        data.get("status_message_id"),
    ))

    conn.commit()
    conn.close()


def load_giveaway() -> dict:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT active, media_type, media_file_id, description, start_mode,
               start_datetime, end_mode, end_value, end_datetime,
               winners_count, code, created_at, post_message_id, status_message_id
        FROM giveaway
        WHERE id = 1
    """)
    row = cur.fetchone()
    conn.close()

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
    conn = get_conn()
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
            created_at = NULL,
            post_message_id = NULL,
            status_message_id = NULL
        WHERE id = 1
    """)

    conn.commit()
    conn.close()


def add_participant(user_id: int, username: str, chosen_number: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    ok = True

    try:
        cur.execute("""
            INSERT INTO participants (user_id, username, joined_at, chosen_number)
            VALUES (?, ?, ?, ?)
        """, (user_id, username, now_utc3().isoformat(), chosen_number))
        conn.commit()
    except sqlite3.IntegrityError:
        ok = False
    finally:
        conn.close()

    return ok


def participant_exists(user_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM participants WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def get_participant_number(user_id: int) -> Optional[int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT chosen_number FROM participants WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] is not None else None


def number_is_taken(number: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM participants WHERE chosen_number = ?", (number,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def get_current_number_max() -> int:
    if current_giveaway.get("end_mode") == "count":
        end_value = current_giveaway.get("end_value")
        if isinstance(end_value, int) and end_value > 0:
            return end_value
    return DEFAULT_NUMBER_MAX


def get_free_numbers() -> List[int]:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT chosen_number FROM participants WHERE chosen_number IS NOT NULL")
    taken_rows = cur.fetchall()

    conn.close()

    taken = {row[0] for row in taken_rows if row[0] is not None}
    max_number = get_current_number_max()
    return [n for n in range(NUMBER_MIN, max_number + 1) if n not in taken]


def get_participants() -> List[Tuple[int, str, str, Optional[int]]]:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT user_id, username, joined_at, chosen_number
        FROM participants
        ORDER BY joined_at ASC
    """)
    rows = cur.fetchall()

    conn.close()
    return rows


def get_participants_count() -> int:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM participants")
    count = cur.fetchone()[0]

    conn.close()
    return count


def clear_participants():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM participants")

    conn.commit()
    conn.close()


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


def admin_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🎁 Новый розыгрыш", callback_data="admin_new"),
        InlineKeyboardButton("🔥 Активный розыгрыш", callback_data="admin_active"),
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
    kb.add(
        InlineKeyboardButton("⚡ Сейчас", callback_data="start_now"),
        InlineKeyboardButton("🕒 По времени", callback_data="start_schedule"),
    )
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
        reply_markup=cancel_kb()
    )


async def send_step_hint_cb(callback: types.CallbackQuery, text: str):
    await callback.message.answer(
        f"{text}\n\nЕсли передумал — нажми «Отмена».",
        reply_markup=cancel_kb()
    )


async def safe_edit_message(chat_id: str, message_id: int, text: str):
    if not message_id:
        return

    for _ in range(3):
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
            return
        except MessageNotModified:
            return
        except RetryAfter as e:
            await asyncio.sleep(float(e.timeout) + 0.5)
        except Exception as e:
            if "Message is not modified" in str(e):
                return
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
                logging.warning(f"Не удалось отредактировать сообщение: {e}")
                return


async def safe_delete_message(chat_id: str, message_id: Optional[int]):
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def animate_winner_selection(participants: List[Tuple[int, str, str, Optional[int]]]) -> int:
    msg = await bot.send_message(ADMIN_ID, "🎲 Запускаю рандомайзер...")

    steps = [
        "🎲 Проверяю участников...",
        "🎲 Считаю шансы...",
        "🎲 Выбираю случайно...",
    ]

    for step in steps:
        await asyncio.sleep(0.8)
        await safe_edit_message(ADMIN_ID, msg.message_id, step)

    preview_names = []
    for _, username, _, chosen_number in participants:
        if chosen_number is not None:
            preview_names.append(f"{username} — №{chosen_number}")
        else:
            preview_names.append(username)

    random.shuffle(preview_names)
    preview_names = preview_names[:min(3, len(preview_names))]

    for name in preview_names:
        await asyncio.sleep(0.6)
        await safe_edit_message(ADMIN_ID, msg.message_id, f"🎲 {name}")

    return msg.message_id


PRE_FINAL_FRAMES_TIME = [
    "⏳ До конца: меньше минуты\n\n🔥 Финал уже близко...",
    "⏳ До конца: меньше минуты\n\n🎲 Уже почти разыгрываем...",
    "⏳ До конца: меньше минуты\n\n👀 Следим за последними секундами...",
    "⏳ До конца: меньше минуты\n\n⚡ Скоро узнаем победителя..."
]

PRE_FINAL_FRAMES_COUNT = [
    "👥 Осталось мест: 1\n\n🔥 Финал уже близко...",
    "👥 Осталось мест: 1\n\n🎲 Уже почти разыгрываем...",
    "👥 Осталось мест: 1\n\n👀 Последнее место почти занято...",
    "👥 Осталось мест: 1\n\n⚡ Скоро узнаем победителя..."
]

FINAL_ANIMATION_FRAMES = [
    "🔥 Финальный этап...",
    "🎲 Определяем победителя...",
    "👀 Уже почти...",
    "⚡ Последние секунды...",
    "🏁 Финал розыгрыша..."
]


def is_prefinal_state() -> bool:
    if not current_giveaway.get("active"):
        return False

    if current_giveaway.get("end_mode") == "time":
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if not end_dt:
            return False
        return (end_dt - now_utc3()).total_seconds() <= 60

    if current_giveaway.get("end_mode") == "count":
        total_needed = current_giveaway.get("end_value", 0) or 0
        left = max(total_needed - get_participants_count(), 0)
        return left <= 1

    return False


def build_status_text() -> str:
    if current_giveaway.get("active"):
        if current_giveaway.get("end_mode") == "time":
            end_dt = parse_dt(current_giveaway.get("end_datetime"))
            if end_dt and (end_dt - now_utc3()).total_seconds() <= 60:
                return random.choice(PRE_FINAL_FRAMES_TIME)
            return f"⏳ До конца розыгрыша: {format_remaining_time(end_dt)}"

        total_needed = current_giveaway.get("end_value", 0) or 0
        current_count = get_participants_count()
        left = max(total_needed - current_count, 0)
        if left <= 1:
            return random.choice(PRE_FINAL_FRAMES_COUNT)
        return f"👥 Осталось мест: {left}"

    if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
        return f"⏳ До старта розыгрыша: {format_remaining_time(parse_dt(current_giveaway.get('start_datetime')))}"

    return "ℹ️ Розыгрыш не активен"


async def publish_status_message():
    status_text = build_status_text()
    msg = await bot.send_message(CHANNEL_ID, status_text)
    current_giveaway["status_message_id"] = msg.message_id
    save_giveaway(current_giveaway)


async def update_status_message():
    status_message_id = current_giveaway.get("status_message_id")
    if not status_message_id:
        return
    await safe_edit_message(CHANNEL_ID, status_message_id, build_status_text())


async def start_status_updates():
    try:
        scheduler.remove_job(STATUS_JOB_ID)
    except Exception:
        pass

    scheduler.add_job(
        update_status_message,
        trigger="interval",
        seconds=5,
        id=STATUS_JOB_ID,
        replace_existing=True
    )


async def stop_status_updates():
    try:
        scheduler.remove_job(STATUS_JOB_ID)
    except Exception:
        pass


async def publish_giveaway_post():
    media_type = current_giveaway["media_type"]
    media_file_id = current_giveaway["media_file_id"]
    description = current_giveaway["description"]

    if media_type == "video":
        post = await bot.send_video(CHANNEL_ID, media_file_id, caption=description)
    else:
        post = await bot.send_photo(CHANNEL_ID, media_file_id, caption=description)

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


async def animate_winners_in_channel(winner_rows: List[Tuple[int, str, str, Optional[int]]]):
    status_message_id = current_giveaway.get("status_message_id")
    if not status_message_id:
        await publish_status_message()
        status_message_id = current_giveaway.get("status_message_id")

    delay = 0.55
    for i in range(26):  # ~20 секунд с замедлением
        await safe_edit_message(CHANNEL_ID, status_message_id, random.choice(FINAL_ANIMATION_FRAMES))
        await asyncio.sleep(delay)
        if i > 12:
            delay += 0.04

    if len(winner_rows) == 1:
        _, winner_name, _, winner_number = winner_rows[0]
        final_text = (
            f"🏆 Победитель определён!\n\n"
            f"{winner_name}\n"
            f"🎯 Номер: {winner_number}"
        )
    else:
        lines = []
        for i, (_, name, _, num) in enumerate(winner_rows, start=1):
            lines.append(f"{i}. {name} — №{num}")
        final_text = "🏆 Победители:\n\n" + "\n".join(lines)

    await safe_edit_message(CHANNEL_ID, status_message_id, final_text)


async def notify_winner_in_private(user_id: int, place_text: str, chosen_number: Optional[int]) -> bool:
    number_text = f"\nТвой выигравший номер: {chosen_number}" if chosen_number is not None else ""
    try:
        await bot.send_message(
            user_id,
            f"🎉 Поздравляем!\n"
            f"Ты {place_text} в розыгрыше.{number_text}\n\n"
            f"Напиши администратору для получения приза."
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
async def finish_giveaway(reason: str):
    global current_giveaway

    if not current_giveaway.get("active"):
        return

    participants = get_participants()
    winners_count = current_giveaway.get("winners_count", 1) or 1

    await remove_finish_job()
    await stop_status_updates()

    if not participants:
        if current_giveaway.get("status_message_id"):
            await safe_edit_message(
                CHANNEL_ID,
                current_giveaway["status_message_id"],
                "🏁 Розыгрыш завершён\n\nУчастников нет ❌"
            )

        await bot.send_message(
            ADMIN_ID,
            f"Розыгрыш завершён ({reason}), но участников нет ❌",
            reply_markup=admin_menu_kb()
        )
    else:
        msg_id = await animate_winner_selection(participants)

        actual_winners_count = min(winners_count, len(participants))
        winner_rows = random.sample(participants, actual_winners_count)

        medals = ["🥇", "🥈", "🥉"]
        winners_lines = []

        for i, (_, winner_name, _, chosen_number) in enumerate(winner_rows, start=1):
            medal = medals[i - 1] if i <= len(medals) else f"{i}."
            number_text = f" — №{chosen_number}" if chosen_number is not None else ""
            winners_lines.append(f"{medal} {winner_name}{number_text}")

        winners_text = "\n".join(winners_lines)

        notify_lines = []
        for i, (winner_id, winner_name, _, chosen_number) in enumerate(winner_rows, start=1):
            place_text = "победитель" if i == 1 else f"в числе победителей (место {i})"
            sent = await notify_winner_in_private(winner_id, place_text, chosen_number)

            if sent:
                notify_lines.append(f"✅ ЛС отправлено: {winner_name}")
            else:
                notify_lines.append(f"⚠️ Не удалось написать в ЛС: {winner_name}")

        notify_text = "\n".join(notify_lines) if notify_lines else "—"

        await safe_delete_message(ADMIN_ID, msg_id)

        await bot.send_message(
            ADMIN_ID,
            f"🎉 Розыгрыш завершён!\n\n"
            f"Причина: {reason}\n"
            f"Участников: {len(participants)}\n"
            f"Победителей: {actual_winners_count}\n\n"
            f"{winners_text}\n\n"
            f"Личные сообщения:\n{notify_text}",
            reply_markup=admin_menu_kb()
        )

        await animate_winners_in_channel(winner_rows)

    current_giveaway = {"active": False}
    clear_participants()
    clear_giveaway_db()
    pending_number_choice.clear()


async def start_giveaway():
    global current_giveaway, finish_job_id

    clear_participants()
    pending_number_choice.clear()

    current_giveaway["active"] = True
    save_giveaway(current_giveaway)

    await publish_giveaway_post()
    await publish_active_status_under_post()

    if current_giveaway["end_mode"] == "time":
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            finish_job_id = f"finish_{int(time.time())}"
            scheduler.add_job(
                finish_giveaway,
                trigger="date",
                run_date=end_dt,
                args=["вышло время"],
                id=finish_job_id,
                replace_existing=True
            )


async def scheduled_start():
    await start_giveaway()


async def restore_state():
    global current_giveaway, finish_job_id, start_job_id

    current_giveaway = load_giveaway()

    if not current_giveaway:
        current_giveaway = {"active": False}
        return

    if current_giveaway.get("active"):
        if not current_giveaway.get("post_message_id"):
            # активный розыгрыш без поста не должен существовать, но на всякий случай
            await start_giveaway()
            return

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
                        replace_existing=True
                    )
        return

    if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
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
                await start_giveaway()
            else:
                start_job_id = START_JOB_ID_RESTORED
                scheduler.add_job(
                    scheduled_start,
                    trigger="date",
                    run_date=start_dt,
                    id=start_job_id,
                    replace_existing=True
                )


async def on_startup(_):
    await bot.delete_webhook(drop_pending_updates=True)
    init_db()
    scheduler.start()
    await restore_state()
    await bot.send_message(
        ADMIN_ID,
        "Бот запущен ✅\n\nВыбери действие:",
        reply_markup=admin_menu_kb()
    )


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
    await message.answer("Создание розыгрыша отменено ❌", reply_markup=admin_menu_kb())


@dp.message_handler(commands=["cancel"])
async def cancel_cmd(message: types.Message):
    global current_giveaway

    if not is_admin(message.from_user.id):
        return

    await remove_start_job()
    await remove_finish_job()
    await stop_status_updates()

    if current_giveaway.get("status_message_id"):
        await safe_delete_message(CHANNEL_ID, current_giveaway.get("status_message_id"))
    if current_giveaway.get("post_message_id") and not current_giveaway.get("active"):
        await safe_delete_message(CHANNEL_ID, current_giveaway.get("post_message_id"))

    clear_participants()
    clear_giveaway_db()
    current_giveaway = {"active": False}
    pending_number_choice.clear()

    await message.answer("Розыгрыш отменён ❌", reply_markup=admin_menu_kb())


# =========================
# USER COMMANDS
# =========================
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    if not current_giveaway.get("active"):
        await message.answer(
            "Сейчас активного розыгрыша нет ❌\n\n"
            "Когда начнётся новый розыгрыш, здесь появится инструкция для участия."
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


@dp.message_handler(commands=["ikota"])
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


@dp.message_handler(commands=["status"])
async def status_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    max_number = get_current_number_max()

    if not current_giveaway.get("active"):
        if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
            finish_text = (
                f"по количеству: {current_giveaway.get('end_value')}"
                if current_giveaway.get("end_mode") == "count"
                else format_dt(parse_dt(current_giveaway.get("end_datetime")))
            )

            await message.answer(
                f"📅 Запланированный розыгрыш\n\n"
                f"Старт: {format_dt(parse_dt(current_giveaway.get('start_datetime')))}\n"
                f"Завершение: {finish_text}\n"
                f"Победителей: {current_giveaway.get('winners_count')}\n"
                f"Номера: {NUMBER_MIN}–{max_number}\n"
                f"{TIMEZONE_TEXT}",
                reply_markup=admin_menu_kb()
            )
            return

        await message.answer("Сейчас активного розыгрыша нет", reply_markup=admin_menu_kb())
        return

    text = (
        f"📊 Статус розыгрыша\n\n"
        f"Код: {current_giveaway.get('code')}\n"
        f"Участников: {get_participants_count()}\n"
        f"Победителей: {current_giveaway.get('winners_count')}\n"
        f"Номера: {NUMBER_MIN}–{max_number}\n"
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
        "Отправь фото или видео для поста.\n\nМожно прислать как медиа или как файл."
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_active")
async def cb_admin_active(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    max_number = get_current_number_max()

    if not current_giveaway.get("active"):
        if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
            text = (
                f"📅 ЗАПЛАНИРОВАННЫЙ РОЗЫГРЫШ\n\n"
                f"Код: {current_giveaway.get('code')}\n"
                f"Старт: {format_dt(parse_dt(current_giveaway.get('start_datetime')))}\n"
                f"Победителей: {current_giveaway.get('winners_count')}\n"
                f"Номера: {NUMBER_MIN}–{max_number}\n"
            )

            if current_giveaway.get("end_mode") == "count":
                text += f"Завершение: по количеству\nЦель: {current_giveaway.get('end_value')}\n"
            else:
                text += f"Завершение: {format_dt(parse_dt(current_giveaway.get('end_datetime')))}\n"

            text += f"\n{TIMEZONE_TEXT}"

            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("❌ Отменить", callback_data="admin_cancel"))

            await callback.message.answer(text, reply_markup=kb)
            await callback.answer()
            return

        await callback.message.answer("Сейчас активного розыгрыша нет")
        await callback.answer()
        return

    text = (
        f"🔥 АКТИВНЫЙ РОЗЫГРЫШ\n\n"
        f"Код: {current_giveaway.get('code')}\n"
        f"Участников: {get_participants_count()}\n"
        f"Победителей: {current_giveaway.get('winners_count')}\n"
        f"Номера: {NUMBER_MIN}–{max_number}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Цель: {current_giveaway.get('end_value')}"
    else:
        text += f"До: {format_dt(parse_dt(current_giveaway.get('end_datetime')))}\n{TIMEZONE_TEXT}"

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


@dp.callback_query_handler(lambda c: c.data == "admin_status")
async def cb_admin_status(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    max_number = get_current_number_max()

    if not current_giveaway.get("active"):
        if current_giveaway.get("start_mode") == "scheduled" and current_giveaway.get("start_datetime"):
            finish_text = (
                f"по количеству: {current_giveaway.get('end_value')}"
                if current_giveaway.get("end_mode") == "count"
                else format_dt(parse_dt(current_giveaway.get("end_datetime")))
            )

            await callback.message.answer(
                f"📅 Запланированный розыгрыш\n\n"
                f"Старт: {format_dt(parse_dt(current_giveaway.get('start_datetime')))}\n"
                f"Завершение: {finish_text}\n"
                f"Победителей: {current_giveaway.get('winners_count')}\n"
                f"Номера: {NUMBER_MIN}–{max_number}\n"
                f"{TIMEZONE_TEXT}"
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
        f"Номера: {NUMBER_MIN}–{max_number}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Завершение: по количеству\nЦель: {current_giveaway.get('end_value')}"
    else:
        text += f"Завершение: по времени\nДо: {format_dt(parse_dt(current_giveaway.get('end_datetime')))}\n{TIMEZONE_TEXT}"

    await callback.message.answer(text)
    await callback.answer()


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
    global current_giveaway

    if not is_admin(callback.from_user.id):
        return

    await remove_start_job()
    await remove_finish_job()
    await stop_status_updates()

    if current_giveaway.get("status_message_id"):
        await safe_delete_message(CHANNEL_ID, current_giveaway.get("status_message_id"))
    if current_giveaway.get("post_message_id") and not current_giveaway.get("active"):
        await safe_delete_message(CHANNEL_ID, current_giveaway.get("post_message_id"))

    clear_participants()
    clear_giveaway_db()
    current_giveaway = {"active": False}
    pending_number_choice.clear()

    await callback.message.answer("Активный или запланированный розыгрыш отменён ❌", reply_markup=admin_menu_kb())
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "setup_cancel", state="*")
async def cb_setup_cancel(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    await state.finish()
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

    if current_giveaway["start_mode"] == "now":
        await start_giveaway()
        await callback.message.answer("✅ Розыгрыш опубликован", reply_markup=admin_menu_kb())
    else:
        await remove_start_job()

        start_dt = parse_dt(current_giveaway["start_datetime"])

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
            replace_existing=True
        )

        await callback.message.answer(
            f"✅ Розыгрыш запланирован на {format_dt(start_dt)}\n{TIMEZONE_TEXT}",
            reply_markup=admin_menu_kb()
        )

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "publish_cancel")
async def cb_publish_cancel(callback: types.CallbackQuery):
    global current_giveaway

    if not is_admin(callback.from_user.id):
        return

    if current_giveaway.get("status_message_id"):
        await safe_delete_message(CHANNEL_ID, current_giveaway.get("status_message_id"))

    clear_giveaway_db()
    current_giveaway = {"active": False}

    await callback.message.answer("Публикация отменена ❌", reply_markup=admin_menu_kb())
    await callback.answer()


# =========================
# ВЫБОР НОМЕРА ЧЕРЕЗ КНОПКУ СЛУЧАЙНОГО ВЫБОРА
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

    free_numbers = get_free_numbers()
    if not free_numbers:
        pending_number_choice.discard(user_id)
        await state.finish()
        await callback.message.answer("Свободных номеров не осталось ❌")
        await callback.answer()
        return

    data = callback.data

    if data == "picknum_random":
        chosen_number = random.choice(free_numbers)
    else:
        chosen_number = int(data.split("_")[1])

    if chosen_number not in free_numbers:
        await callback.answer("Этот номер уже занят", show_alert=True)
        await ask_for_number_choice(callback, resend=True)
        return

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

    await update_status_message()

    if current_giveaway.get("end_mode") == "count":
        if get_participants_count() >= current_giveaway.get("end_value"):
            await finish_giveaway("набрано нужное количество участников")


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
            await send_step_hint(
                message,
                "Подходит только фото или видео.\n\nОтправь другой файл."
            )
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


@dp.callback_query_handler(lambda c: c.data in ["start_now", "start_schedule"], state=GiveawayForm.waiting_start_mode)
async def process_start_mode(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "start_now":
        await state.update_data(start_mode="now", start_datetime=None)
        await GiveawayForm.waiting_end_mode.set()
        await callback.message.answer("Как завершать розыгрыш?", reply_markup=end_mode_kb())
    else:
        await state.update_data(start_mode="scheduled")
        await GiveawayForm.waiting_start_datetime.set()
        await send_step_hint_cb(
            callback,
            f"Введи дату и время старта в формате:\n01.01.2000 23:59\n\n{current_time_hint()}"
        )
    await callback.answer()


@dp.message_handler(state=GiveawayForm.waiting_start_datetime, content_types=types.ContentTypes.TEXT)
async def process_start_datetime(message: types.Message, state: FSMContext):
    try:
        start_dt = parse_user_datetime(message.text)
    except ValueError:
        await send_step_hint(
            message,
            f"Неверный формат.\nПример: 01.01.2000 23:59\n\n{current_time_hint()}"
        )
        return

    if start_dt <= now_utc3():
        await send_step_hint(
            message,
            f"Время старта должно быть в будущем.\n\n{current_time_hint()}"
        )
        return

    await state.update_data(start_datetime=start_dt.isoformat())
    await GiveawayForm.waiting_end_mode.set()
    await message.answer("Как завершать розыгрыш?", reply_markup=end_mode_kb())


@dp.callback_query_handler(lambda c: c.data in ["end_count", "end_time"], state=GiveawayForm.waiting_end_mode)
async def process_end_mode(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "end_count":
        await state.update_data(end_mode="count", end_datetime=None)
        await GiveawayForm.waiting_end_count.set()
        await send_step_hint_cb(
            callback,
            "Сколько участников должно быть до завершения?"
        )
    else:
        await state.update_data(end_mode="time", end_value=None)
        await GiveawayForm.waiting_end_datetime.set()
        await send_step_hint_cb(
            callback,
            f"Введи дату и время завершения в формате:\n01.01.2000 23:59\n\n{current_time_hint()}"
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
        await send_step_hint(
            message,
            f"Неверный формат.\nПример: 01.01.2000 23:59\n\n{current_time_hint()}"
        )
        return

    data = await state.get_data()
    start_mode = data.get("start_mode")
    start_datetime = data.get("start_datetime")

    if end_dt <= now_utc3():
        await send_step_hint(
            message,
            f"Время завершения должно быть в будущем.\n\n{current_time_hint()}"
        )
        return

    if start_mode == "scheduled" and start_datetime:
        start_dt = parse_dt(start_datetime)
        if start_dt and end_dt <= start_dt:
            await send_step_hint(
                message,
                f"Время завершения должно быть позже времени старта.\n\n{current_time_hint()}"
            )
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

    code = message.text.strip()

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
        await bot.send_video(ADMIN_ID, current_giveaway["media_file_id"], caption=caption)
    else:
        await bot.send_photo(ADMIN_ID, current_giveaway["media_file_id"], caption=caption)

    await message.answer(
        "👇 Так будет выглядеть пост\n\nЕсли всё ок — нажми «Опубликовать».",
        reply_markup=publish_preview_kb()
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
        reply_markup=start_mode_kb()
    )


@dp.message_handler(state=GiveawayForm.waiting_start_datetime)
async def fallback_waiting_start_datetime(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await send_step_hint(
        message,
        f"Сейчас бот ждёт дату и время старта в формате:\n01.01.2000 23:59\n\n{current_time_hint()}"
    )


@dp.message_handler(state=GiveawayForm.waiting_end_mode)
async def fallback_waiting_end_mode(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "Сейчас выбери способ завершения кнопками ниже.\n\nЕсли передумал — нажми «Отмена».",
        reply_markup=end_mode_kb()
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
    await send_step_hint(
        message,
        f"Сейчас бот ждёт дату и время завершения в формате:\n01.01.2000 23:59\n\n{current_time_hint()}"
    )


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
        await message.answer("Сначала отправь код для участия.")
        return

    max_number = get_current_number_max()

    try:
        chosen_number = int((message.text or "").strip())
    except ValueError:
        await message.answer(
            f"❌ Напиши число от {NUMBER_MIN} до {max_number}\n"
            f"🎲 Или нажми «Случайный номер»",
            reply_markup=choose_number_kb()
        )
        return

    if chosen_number < NUMBER_MIN or chosen_number > max_number:
        await message.answer(
            f"❌ Можно выбрать число только от {NUMBER_MIN} до {max_number}",
            reply_markup=choose_number_kb()
        )
        return

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
                reply_markup=choose_number_kb()
            )
        else:
            await message.answer(
                f"⚠️ Номер {chosen_number} уже занят\n"
                f"✍️ Напиши другой номер\n"
                f"🎲 Или нажми «Случайный номер»",
                reply_markup=choose_number_kb()
            )
        return

    username = message.from_user.username
    full_name = message.from_user.full_name
    display_name = f"@{username}" if username else full_name

    ok = add_participant(user_id, display_name, chosen_number)
    if not ok:
        await message.answer(
            "Этот номер только что заняли ❌\nВыбери другой.",
            reply_markup=choose_number_kb()
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

    await update_status_message()

    if current_giveaway.get("end_mode") == "count":
        if get_participants_count() >= current_giveaway.get("end_value"):
            await finish_giveaway("набрано нужное количество участников")


# =========================
# УЧАСТИЕ ПОЛЬЗОВАТЕЛЯ
# =========================
@dp.message_handler()
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
    current_code = current_giveaway.get("code")

    if text != current_code:
        register_wrong_code_attempt(user_id)
        await message.answer("Неверный код ❌\nПроверь внимательно и попробуй ещё раз")
        return

    if participant_exists(user_id):
        chosen_number = get_participant_number(user_id)
        number_text = f"\nТвой номер: {chosen_number}" if chosen_number is not None else ""
        await message.answer(f"Ты уже участвуешь в этом розыгрыше 🎉{number_text}")
        return

    pending_number_choice.add(user_id)
    await NumberChoiceForm.waiting_number.set()
    await ask_for_number_choice(message)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)