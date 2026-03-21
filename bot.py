import os
import time
import random
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
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

MESSAGE_COOLDOWN_SECONDS = 2
MAX_WRONG_CODE_ATTEMPTS = 5
WRONG_CODE_BLOCK_MINUTES = 10

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler()

print("БОТ ЗАПУЩЕН ✅")


# =========================
# ВРЕМЕННОЕ СОСТОЯНИЕ
# =========================
last_message_time = {}      # user_id -> timestamp
wrong_code_attempts = {}    # user_id -> {"count": int, "until": timestamp}
pending_confirm = set()     # user_id, кому показана кнопка "Я человек"

current_giveaway = {}
finish_job_id = None


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
    conn.close()


def load_giveaway() -> dict:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT active, media_type, media_file_id, description, start_mode,
               start_datetime, end_mode, end_value, end_datetime,
               winners_count, code, created_at
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
            created_at = NULL
        WHERE id = 1
    """)

    conn.commit()
    conn.close()


def add_participant(user_id: int, username: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO participants (user_id, username, joined_at)
        VALUES (?, ?, ?)
    """, (user_id, username, datetime.now().isoformat()))

    conn.commit()
    conn.close()


def participant_exists(user_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM participants WHERE user_id = ?", (user_id,))
    row = cur.fetchone()

    conn.close()
    return row is not None


def get_participants() -> List[Tuple[int, str, str]]:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT user_id, username, joined_at
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


def mark_confirm_passed(user_id: int, giveaway_code: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO passed_confirm (user_id, giveaway_code, passed_at)
        VALUES (?, ?, ?)
    """, (user_id, giveaway_code, datetime.now().isoformat()))

    conn.commit()
    conn.close()


def has_passed_confirm(user_id: int, giveaway_code: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT 1 FROM passed_confirm
        WHERE user_id = ? AND giveaway_code = ?
    """, (user_id, giveaway_code))
    row = cur.fetchone()

    conn.close()
    return row is not None


def clear_passed_confirm():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM passed_confirm")

    conn.commit()
    conn.close()


# =========================
# HELPERS
# =========================
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)


def build_caption(description: str) -> str:
    # ВАЖНО: бот больше ничего не дописывает от себя
    return description


def user_rate_limited(user_id: int) -> Optional[str]:
    now = time.time()

    if user_id in wrong_code_attempts:
        blocked_until = wrong_code_attempts[user_id]["until"]
        if blocked_until > now:
            minutes_left = int((blocked_until - now) // 60) + 1
            return f"Слишком много неверных попыток. Попробуй через {minutes_left} мин."

    last_ts = last_message_time.get(user_id, 0)
    if now - last_ts < MESSAGE_COOLDOWN_SECONDS:
        return "Слишком быстро. Подожди пару секунд."

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
        InlineKeyboardButton("🎁 Новый розыгрыш", callback_data="admin_new"),
        InlineKeyboardButton("📊 Статус", callback_data="admin_status"),
        InlineKeyboardButton("🧾 Список участников", callback_data="admin_list"),
        InlineKeyboardButton("❌ Отменить розыгрыш", callback_data="admin_cancel"),
    )
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


def confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_yes"),
        InlineKeyboardButton("❌ Отмена", callback_data="confirm_no"),
    )
    return kb


def human_check_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Я человек ✅", callback_data="captcha_ok"))
    return kb


async def animate_winner_selection(participants: List[Tuple[int, str, str]]) -> int:
    msg = await bot.send_message(ADMIN_ID, "🎲 Запускаю рандомайзер...")

    steps = [
        "🎲 Проверяю участников...",
        "🎲 Считаю шансы...",
        "🎲 Выбираю случайно...",
    ]

    for step in steps:
        await asyncio.sleep(0.8)
        await msg.edit_text(step)

    preview_names = [p[1] for p in participants]
    random.shuffle(preview_names)
    preview_names = preview_names[:min(3, len(preview_names))]

    for name in preview_names:
        await asyncio.sleep(0.6)
        await msg.edit_text(f"🎲 {name}")

    return msg.message_id


async def post_winners_to_channel(winners_text: str):
    try:
        await bot.send_message(
            CHANNEL_ID,
            f"🏆 РЕЗУЛЬТАТЫ РОЗЫГРЫША\n\n{winners_text}"
        )
    except Exception as e:
        await bot.send_message(ADMIN_ID, f"Не удалось отправить победителей в канал: {e}")


async def remove_finish_job():
    global finish_job_id
    if finish_job_id:
        try:
            scheduler.remove_job(finish_job_id)
        except Exception:
            pass
        finish_job_id = None


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

    if not participants:
        await bot.send_message(
            ADMIN_ID,
            f"Розыгрыш завершён ({reason}), но участников нет ❌"
        )
        await post_winners_to_channel("Участников не было.")
    else:
        message_id = await animate_winner_selection(participants)

        ids = [row[0] for row in participants]
        names = {row[0]: row[1] for row in participants}

        actual_winners_count = min(winners_count, len(ids))
        winner_ids = random.sample(ids, actual_winners_count)

        winners_lines = []
        for i, winner_id in enumerate(winner_ids, start=1):
            winners_lines.append(f"{i}. {names[winner_id]}")

        winners_text = "\n".join(winners_lines)

        await bot.edit_message_text(
            f"🏆 Победитель определён!\n\n{winners_text}",
            chat_id=ADMIN_ID,
            message_id=message_id
        )

        await bot.send_message(
            ADMIN_ID,
            f"🎉 Розыгрыш завершён!\n\n"
            f"Причина: {reason}\n"
            f"Участников: {len(participants)}\n"
            f"Победителей: {actual_winners_count}\n\n"
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

    clear_participants()
    clear_passed_confirm()
    pending_confirm.clear()

    media_type = current_giveaway["media_type"]
    media_file_id = current_giveaway["media_file_id"]
    description = current_giveaway["description"]
    caption = build_caption(description)

    current_giveaway["active"] = True
    save_giveaway(current_giveaway)

    if media_type == "video":
        await bot.send_video(
            chat_id=CHANNEL_ID,
            video=media_file_id,
            caption=caption
        )
    else:
        await bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=media_file_id,
            caption=caption
        )

    await bot.send_message(ADMIN_ID, "✅ Розыгрыш запущен и опубликован в канале")

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
    global current_giveaway, finish_job_id

    current_giveaway = load_giveaway()

    if not current_giveaway:
        current_giveaway = {"active": False}
        return

    if current_giveaway.get("active") and current_giveaway.get("end_mode") == "time":
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            if datetime.now() >= end_dt:
                await finish_giveaway("вышло время после перезапуска")
            else:
                finish_job_id = "finish_restored"
                scheduler.add_job(
                    finish_giveaway,
                    trigger="date",
                    run_date=end_dt,
                    args=["вышло время"],
                    id=finish_job_id,
                    replace_existing=True
                )


async def on_startup(_):
    init_db()
    scheduler.start()
    await restore_state()
    await bot.send_message(ADMIN_ID, "Планировщик запущен ✅")


# =========================
# USER COMMANDS
# =========================
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    await message.answer(
        "Привет 👋\n\n"
        "Чтобы участвовать:\n"
        "1. Подпишись на канал\n"
        "2. Найди код\n"
        "3. Отправь код сюда\n"
        "4. Нажми кнопку подтверждения"
    )


@dp.message_handler(commands=["admin"])
async def admin_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("⚙️ Админ-меню", reply_markup=admin_menu_kb())


@dp.message_handler(commands=["status"])
async def status_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    if not current_giveaway.get("active"):
        await message.answer("Сейчас активного розыгрыша нет")
        return

    text = (
        f"📊 Статус розыгрыша\n\n"
        f"Код: {current_giveaway.get('code')}\n"
        f"Участников: {get_participants_count()}\n"
        f"Победителей: {current_giveaway.get('winners_count')}\n"
        f"Завершение: {'по количеству' if current_giveaway.get('end_mode') == 'count' else 'по времени'}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Цель: {current_giveaway.get('end_value')}"
    else:
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            text += f"До: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}"

    await message.answer(text)


@dp.message_handler(commands=["cancel"])
async def cancel_cmd(message: types.Message):
    global current_giveaway

    if not is_admin(message.from_user.id):
        return

    await remove_finish_job()

    clear_participants()
    clear_passed_confirm()
    clear_giveaway_db()
    current_giveaway = {"active": False}
    pending_confirm.clear()

    await message.answer("Активный розыгрыш отменён ❌")


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

    await GiveawayForm.waiting_media.set()
    await callback.message.answer(
        "Отправь фото или видео для поста.\n\n"
        "Можно прислать как медиа или как файл."
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_status")
async def cb_admin_status(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    if not current_giveaway.get("active"):
        await callback.message.answer("Сейчас активного розыгрыша нет")
        await callback.answer()
        return

    text = (
        f"📊 Статус розыгрыша\n\n"
        f"Код: {current_giveaway.get('code')}\n"
        f"Участников: {get_participants_count()}\n"
        f"Победителей: {current_giveaway.get('winners_count')}\n"
        f"Завершение: {'по количеству' if current_giveaway.get('end_mode') == 'count' else 'по времени'}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Цель: {current_giveaway.get('end_value')}"
    else:
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            text += f"До: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}"

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
    for i, (_, username, joined_at) in enumerate(participants[:50], start=1):
        lines.append(f"{i}. {username} — {joined_at[:19].replace('T', ' ')}")

    if len(participants) > 50:
        lines.append(f"\nИ ещё {len(participants) - 50} участников...")

    await callback.message.answer("\n".join(lines))
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "admin_cancel")
async def cb_admin_cancel(callback: types.CallbackQuery):
    global current_giveaway

    if not is_admin(callback.from_user.id):
        return

    await remove_finish_job()

    clear_participants()
    clear_passed_confirm()
    clear_giveaway_db()
    current_giveaway = {"active": False}
    pending_confirm.clear()

    await callback.message.answer("Активный розыгрыш отменён ❌")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "setup_cancel", state="*")
async def cb_setup_cancel(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    await state.finish()
    await callback.message.answer("Создание розыгрыша отменено ❌")
    await callback.answer()


# =========================
# СОЗДАНИЕ РОЗЫГРЫША
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
            await message.answer("Подходит только фото или видео")
            return
        file_id = message.document.file_id
    else:
        await message.answer("Отправь фото или видео")
        return

    await state.update_data(media_type=media_type, media_file_id=file_id)
    await GiveawayForm.waiting_description.set()
    await message.answer("Теперь отправь описание поста")


@dp.message_handler(state=GiveawayForm.waiting_description)
async def process_description(message: types.Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
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
        await callback.message.answer("Введи дату и время старта в формате:\n2026-03-20 21:30")
    await callback.answer()


@dp.message_handler(state=GiveawayForm.waiting_start_datetime)
async def process_start_datetime(message: types.Message, state: FSMContext):
    try:
        start_dt = datetime.strptime(message.text.strip(), "%Y-%m-%d %H:%M")
    except ValueError:
        await message.answer("Неверный формат. Пример: 2026-03-20 21:30")
        return

    if start_dt <= datetime.now():
        await message.answer("Время старта должно быть в будущем")
        return

    await state.update_data(start_datetime=start_dt.isoformat())
    await GiveawayForm.waiting_end_mode.set()
    await message.answer("Как завершать розыгрыш?", reply_markup=end_mode_kb())


@dp.callback_query_handler(lambda c: c.data in ["end_count", "end_time"], state=GiveawayForm.waiting_end_mode)
async def process_end_mode(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "end_count":
        await state.update_data(end_mode="count")
        await callback.message.answer("Сколько участников должно быть до завершения?")
    else:
        await state.update_data(end_mode="time")
        await callback.message.answer("Сколько минут должен идти розыгрыш после старта?")
    await GiveawayForm.waiting_end_value.set()
    await callback.answer()


@dp.message_handler(state=GiveawayForm.waiting_end_value)
async def process_end_value(message: types.Message, state: FSMContext):
    try:
        value = int(message.text.strip())
    except ValueError:
        await message.answer("Нужно ввести число")
        return

    if value <= 0:
        await message.answer("Число должно быть больше нуля")
        return

    await state.update_data(end_value=value)
    await GiveawayForm.waiting_winners_count.set()
    await message.answer("Сколько победителей выбрать?")


@dp.message_handler(state=GiveawayForm.waiting_winners_count)
async def process_winners_count(message: types.Message, state: FSMContext):
    try:
        winners_count = int(message.text.strip())
    except ValueError:
        await message.answer("Нужно ввести число")
        return

    if winners_count <= 0:
        await message.answer("Количество победителей должно быть больше нуля")
        return

    await state.update_data(winners_count=winners_count)
    await GiveawayForm.waiting_code.set()
    await message.answer("Теперь отправь код, который участники будут вводить в бота")


@dp.message_handler(state=GiveawayForm.waiting_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    if not code:
        await message.answer("Код не должен быть пустым")
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

    start_text = "сейчас" if start_mode == "now" else datetime.fromisoformat(start_datetime).strftime("%Y-%m-%d %H:%M")
    end_text = "по количеству" if end_mode == "count" else "по времени"

    summary = (
        f"Проверь настройки розыгрыша:\n\n"
        f"Файл: {media_type}\n"
        f"Описание: {description}\n"
        f"Старт: {start_text}\n"
        f"Завершение: {end_text}\n"
        f"Значение: {end_value}\n"
        f"Победителей: {winners_count}\n"
        f"Код: {code}"
    )

    await GiveawayForm.waiting_confirm.set()
    await message.answer(summary, reply_markup=confirm_kb())


@dp.callback_query_handler(lambda c: c.data in ["confirm_yes", "confirm_no"], state=GiveawayForm.waiting_confirm)
async def process_confirm(callback: types.CallbackQuery, state: FSMContext):
    global current_giveaway

    if callback.data == "confirm_no":
        await state.finish()
        await callback.message.answer("Создание розыгрыша отменено ❌")
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
        "created_at": datetime.now().isoformat(),
    }

    if current_giveaway["end_mode"] == "time":
        if current_giveaway["start_mode"] == "now":
            current_giveaway["end_datetime"] = (
                datetime.now() + timedelta(minutes=current_giveaway["end_value"])
            ).isoformat()
        else:
            start_dt = datetime.fromisoformat(current_giveaway["start_datetime"])
            current_giveaway["end_datetime"] = (
                start_dt + timedelta(minutes=current_giveaway["end_value"])
            ).isoformat()
    else:
        current_giveaway["end_datetime"] = None

    save_giveaway(current_giveaway)
    await state.finish()

    if current_giveaway["start_mode"] == "now":
        await start_giveaway()
    else:
        scheduler.add_job(
            scheduled_start,
            trigger="date",
            run_date=datetime.fromisoformat(current_giveaway["start_datetime"])
        )
        await callback.message.answer(
            f"✅ Розыгрыш запланирован на "
            f"{datetime.fromisoformat(current_giveaway['start_datetime']).strftime('%Y-%m-%d %H:%M')}"
        )

    await callback.answer()


# =========================
# ПОДТВЕРЖДЕНИЕ УЧАСТИЯ
# =========================
@dp.callback_query_handler(lambda c: c.data == "captcha_ok")
async def captcha_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    current_code = current_giveaway.get("code")

    if user_id not in pending_confirm:
        await callback.answer("Подтверждение уже неактуально", show_alert=True)
        return

    username = callback.from_user.username
    full_name = callback.from_user.full_name
    display_name = f"@{username}" if username else full_name

    if not participant_exists(user_id):
        add_participant(user_id, display_name)

    mark_confirm_passed(user_id, current_code)
    pending_confirm.discard(user_id)
    reset_wrong_code_attempts(user_id)

    await callback.message.edit_text("Ты подтверждён и участвуешь 🎉")
    await callback.answer("Готово!")

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

    text = message.text.strip()
    current_code = current_giveaway.get("code")

    if text != current_code:
        register_wrong_code_attempt(user_id)
        await message.answer("Неверный код ❌")
        return

    if participant_exists(user_id):
        await message.answer("Ты уже участвуешь 😉")
        return

    if has_passed_confirm(user_id, current_code):
        username = message.from_user.username
        full_name = message.from_user.full_name
        display_name = f"@{username}" if username else full_name

        add_participant(user_id, display_name)
        await message.answer("Ты участвуешь 🎉")
        return

    pending_confirm.add(user_id)
    await message.answer(
        "Подтверди, что ты человек 👇",
        reply_markup=human_check_kb()
    )


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)